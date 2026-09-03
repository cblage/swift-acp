//
//  ProcessManager.swift
//  ACP
//
//  Manages subprocess lifecycle, I/O pipes, and message serialization
//

#if os(macOS)
import Foundation
import Darwin
import os.log
import ACPModel

actor ACPProcessManager {
    // MARK: - Properties

    private var process: Process?
    private var processGroupId: pid_t?
    private var stdinPipe: Pipe?
    private var stdoutPipe: Pipe?
    private var stderrPipe: Pipe?

    private var readBuffer: Data = Data()
    /// Bytes of `readBuffer` already handed out — ADVANCED, never shifted
    /// out with `removeFirst`, and compacted only once they dominate the
    /// buffer, so consuming a message costs its own length, amortised.
    private var readOffset = 0
    /// The framer's position and state, kept ACROSS chunks: a message that
    /// arrives in pieces is scanned once, resuming where the last chunk
    /// ended. The old framer copied the whole buffer into an array and
    /// rescanned it from byte zero on every chunk, so a large frame cost
    /// its size squared and a session's reader task ran flat out for the
    /// length of a stream.
    private var scanIndex = 0
    private var scanDepth = 0
    private var scanInString = false
    private var scanEscaped = false
    private var largeBufferDumpCount: Int = 0
    private var lastLargeBufferDumpSize: Int = 0

    private let encoder: JSONEncoder
    private let decoder: JSONDecoder
    private let logger: Logger

    private static let largeBufferWarningThreshold = 200000
    private static let largeBufferDumpMinGrowth = 8192
    private static let maxLargeBufferDumps = 3

    private var onDataReceived: ((Data) async -> Void)?
    private var onTermination: ((Int32) async -> Void)?

    private enum OutputChunk: Sendable {
        case stdout(Data)
        case stderr(Data)
    }

    private var outputContinuation: AsyncStream<OutputChunk>.Continuation?
    private var outputConsumerTask: Task<Void, Never>?
    private var stderrLineContinuation: AsyncStream<String>.Continuation?
    private var stderrLineStream: AsyncStream<String>?
    private var stderrBuffer = Data()

    // MARK: - Initialization

    init(encoder: JSONEncoder, decoder: JSONDecoder) {
        self.encoder = encoder
        self.decoder = decoder
        self.logger = Logger.forCategory("ACPProcessManager")
    }

    // MARK: - Process Lifecycle

    func launch(agentPath: String, arguments: [String] = [], workingDirectory: String? = nil, environment customEnvironment: [String: String]? = nil) throws {
        guard process == nil else {
            throw ClientError.invalidResponse
        }

        let proc = Process()

        let resolvedPath = (try? FileManager.default.destinationOfSymbolicLink(atPath: agentPath)) ?? agentPath
        let actualPath = resolvedPath.hasPrefix("/") ? resolvedPath : ((agentPath as NSString).deletingLastPathComponent as NSString).appendingPathComponent(resolvedPath)

        let isNodeScript: Bool = {
            guard let handle = FileHandle(forReadingAtPath: actualPath) else { return false }
            defer { try? handle.close() }
            guard let data = try? handle.read(upToCount: 64),
                  let firstLine = String(data: data, encoding: .utf8) else { return false }
            return firstLine.hasPrefix("#!/usr/bin/env node")
        }()

        if isNodeScript {
            let searchPaths = [
                (agentPath as NSString).deletingLastPathComponent,
                (actualPath as NSString).deletingLastPathComponent,
                "/opt/homebrew/bin",
                "/usr/local/bin",
                "/usr/bin"
            ]

            var foundNode: String?
            for searchPath in searchPaths {
                let nodePath = (searchPath as NSString).appendingPathComponent("node")
                if FileManager.default.fileExists(atPath: nodePath) {
                    foundNode = nodePath
                    break
                }
            }

            if let nodePath = foundNode {
                proc.executableURL = URL(fileURLWithPath: nodePath)
                proc.arguments = [actualPath] + arguments
            } else {
                proc.executableURL = URL(fileURLWithPath: agentPath)
                proc.arguments = arguments
            }
        } else {
            proc.executableURL = URL(fileURLWithPath: agentPath)
            proc.arguments = arguments
        }

        var environment = ShellEnvironment.loadUserShellEnvironment()

        // Merge custom environment variables (override shell env)
        if let customEnvironment {
            for (key, value) in customEnvironment {
                environment[key] = value
            }
        }

        if let workingDirectory, !workingDirectory.isEmpty {
            environment["PWD"] = workingDirectory
            environment["OLDPWD"] = workingDirectory
            proc.currentDirectoryURL = URL(fileURLWithPath: workingDirectory)
        }

        let agentDir = (agentPath as NSString).deletingLastPathComponent

        if let existingPath = environment["PATH"] {
            environment["PATH"] = "\(agentDir):\(existingPath)"
        } else {
            environment["PATH"] = agentDir
        }

        proc.environment = environment

        let stdin = Pipe()
        let stdout = Pipe()
        let stderr = Pipe()

        proc.standardInput = stdin
        proc.standardOutput = stdout
        proc.standardError = stderr

        stdinPipe = stdin
        stdoutPipe = stdout
        stderrPipe = stderr

        proc.terminationHandler = { [weak self] process in
            Task {
                await self?.handleTermination(exitCode: process.terminationStatus)
            }
        }

        try proc.run()
        process = proc
        processGroupId = nil
        if proc.processIdentifier > 0 {
            let pid = proc.processIdentifier
            if setpgid(pid, pid) == 0 {
                processGroupId = pid
            } else {
                logger.warning("Failed to set process group for pid=\(pid): \(String(cString: strerror(errno)))")
            }
        }
        if proc.processIdentifier > 0 {
            let pid = proc.processIdentifier
            let pgid = processGroupId
            Task {
                await ProcessRegistry.shared.recordProcess(pid: pid, pgid: pgid, agentPath: actualPath)
            }
        }

        startOutputProcessing()
        startReading()
        startReadingStderr()
    }

    func isRunning() -> Bool {
        return process?.isRunning == true
    }

    func processIdentifier() -> Int32? {
        guard process?.isRunning == true, let pid = process?.processIdentifier, pid > 0 else {
            return nil
        }
        return pid
    }

    func processGroupIdentifier() -> Int32? {
        guard process?.isRunning == true else { return nil }
        return processGroupId
    }

    func stderrLines() -> AsyncStream<String>? {
        guard process != nil else { return nil }
        return stderrLineStream
    }

    func terminate() async {
        let proc = process
        let pgid = processGroupId
        let pid = proc?.processIdentifier

        stdoutPipe?.fileHandleForReading.readabilityHandler = nil
        stderrPipe?.fileHandleForReading.readabilityHandler = nil

        try? stdinPipe?.fileHandleForWriting.close()
        try? stdoutPipe?.fileHandleForReading.close()
        try? stderrPipe?.fileHandleForReading.close()

        await finishOutputProcessing()

        if let proc, proc.isRunning {
            if let pgid {
                _ = killpg(pgid, SIGTERM)
            } else {
                proc.terminate()
            }
        }

        if let proc {
            let exited = await waitForExit(proc, timeout: 2.0)
            if !exited, proc.processIdentifier > 0 {
                if let pgid {
                    _ = killpg(pgid, SIGKILL)
                } else {
                    _ = kill(proc.processIdentifier, SIGKILL)
                }
            }
        }
        await ProcessRegistry.shared.removeProcess(pid: pid, pgid: pgid)
        process = nil
        processGroupId = nil

        stdinPipe = nil
        stdoutPipe = nil
        stderrPipe = nil

        resetReadState()
    }

    // MARK: - I/O Operations

    func writeMessage<T: Encodable>(_ message: T) async throws {
        guard let stdin = stdinPipe?.fileHandleForWriting else {
            throw ClientError.processNotRunning
        }

        let data = try encoder.encode(message)

        var lineData = data
        lineData.append(0x0A)

        try stdin.write(contentsOf: lineData)
    }

    // MARK: - Callbacks

    func setDataReceivedCallback(_ callback: @escaping (Data) async -> Void) {
        self.onDataReceived = callback
    }

    func setTerminationCallback(_ callback: @escaping (Int32) async -> Void) {
        self.onTermination = callback
    }

    // MARK: - Private Methods

    private func startOutputProcessing() {
        var stderrContinuation: AsyncStream<String>.Continuation!
        stderrLineStream = AsyncStream { stderrContinuation = $0 }
        stderrLineContinuation = stderrContinuation
        stderrBuffer.removeAll(keepingCapacity: true)

        var outputContinuation: AsyncStream<OutputChunk>.Continuation!
        let outputStream = AsyncStream<OutputChunk>(bufferingPolicy: .unbounded) {
            outputContinuation = $0
        }
        self.outputContinuation = outputContinuation
        outputConsumerTask = Task { [weak self] in
            for await chunk in outputStream {
                guard let self else { return }
                await self.processOutput(chunk)
            }
        }
    }

    private func startReading() {
        guard let stdout = stdoutPipe?.fileHandleForReading,
              let outputContinuation else { return }

        stdout.readabilityHandler = { handle in
            let data = handle.availableData

            guard !data.isEmpty else {
                handle.readabilityHandler = nil
                return
            }

            outputContinuation.yield(.stdout(data))
        }
    }

    private func startReadingStderr() {
        guard let stderr = stderrPipe?.fileHandleForReading,
              let outputContinuation else { return }

        stderr.readabilityHandler = { handle in
            let data = handle.availableData
            guard !data.isEmpty else {
                handle.readabilityHandler = nil
                return
            }
            outputContinuation.yield(.stderr(data))
        }
    }

    private func processOutput(_ chunk: OutputChunk) async {
        switch chunk {
        case .stdout(let data):
            await processIncomingData(data)
        case .stderr(let data):
            processStderrData(data)
        }
    }

    private func processStderrData(_ data: Data) {
        stderrBuffer.append(data)

        while let newlineIndex = stderrBuffer.firstIndex(of: 0x0A) {
            var line = Data(stderrBuffer[..<newlineIndex])
            let removeCount = stderrBuffer.distance(from: stderrBuffer.startIndex, to: newlineIndex) + 1
            stderrBuffer.removeFirst(min(removeCount, stderrBuffer.count))
            if line.last == 0x0D {
                line.removeLast()
            }
            stderrLineContinuation?.yield(String(decoding: line, as: UTF8.self))
        }
    }

    private func finishOutputProcessing() async {
        outputContinuation?.finish()
        if let outputConsumerTask {
            await outputConsumerTask.value
        }
        outputConsumerTask = nil
        outputContinuation = nil

        if !stderrBuffer.isEmpty {
            stderrLineContinuation?.yield(String(decoding: stderrBuffer, as: UTF8.self))
            stderrBuffer.removeAll(keepingCapacity: true)
        }
        stderrLineContinuation?.finish()
        stderrLineContinuation = nil
        stderrLineStream = nil
    }

    private func processIncomingData(_ data: Data) async {
        readBuffer.append(data)

        await drainBufferedMessages()
    }

    private func handleTermination(exitCode: Int32) async {
        let pid = process?.processIdentifier
        let pgid = processGroupId
        await drainAndClosePipes()
        logger.info("Agent process terminated with code: \(exitCode)")
        await ProcessRegistry.shared.removeProcess(pid: pid, pgid: pgid)
        await onTermination?(exitCode)
    }

    private func drainAndClosePipes() async {
        if let stdoutHandle = stdoutPipe?.fileHandleForReading {
            stdoutHandle.readabilityHandler = nil
            do {
                while true {
                    guard let chunk = try stdoutHandle.read(upToCount: 65536), !chunk.isEmpty else {
                        break
                    }
                    outputContinuation?.yield(.stdout(chunk))
                }
            } catch {
                // Handle already closed or invalid file handles safely
            }
            try? stdoutHandle.close()
        }

        if let stderrHandle = stderrPipe?.fileHandleForReading {
            stderrHandle.readabilityHandler = nil
            do {
                while true {
                    guard let chunk = try stderrHandle.read(upToCount: 65536), !chunk.isEmpty else {
                        break
                    }
                    outputContinuation?.yield(.stderr(chunk))
                }
            } catch {
                // Handle already closed or invalid file handles safely
            }
            try? stderrHandle.close()
        }

        await finishOutputProcessing()
        await flushRemainingBufferIfNeeded()

        try? stdinPipe?.fileHandleForWriting.close()

        stdinPipe = nil
        stdoutPipe = nil
        stderrPipe = nil
        process = nil
        processGroupId = nil
        resetReadState()
    }

    // MARK: - JSON Message Parsing

    private func drainBufferedMessages() async {
        while let message = popNextMessage() {
            await onDataReceived?(message)
        }
    }

    /// One complete top-level JSON value from the front of the buffer, or nil
    /// while the buffer holds none. INCREMENTAL: the scanner's index and
    /// state persist across calls, so a message arriving in chunks is walked
    /// once, resuming where the previous chunk ended, and consumed bytes are
    /// skipped by offset rather than shifted out. The old framer copied the
    /// whole buffer into an array, rescanned it from byte zero on every
    /// chunk, parsed each candidate through JSONSerialization just to
    /// validate it, and `removeFirst`-shifted the remainder — four full
    /// passes per message, quadratic for a chunked frame, and a session's
    /// reader task ran flat out for the length of a stream. Validation is
    /// the decoder's job: a malformed frame fails there and is logged.
    private func popNextMessage() -> Data? {
        while true {
            let count = readBuffer.count
            if scanDepth == 0 {
                // At a boundary: skip whitespace and any non-JSON prefix.
                var start = readOffset
                while start < count, Self.isWhitespace(readBuffer[start]) { start += 1 }
                guard start < count else {
                    readOffset = count
                    scanIndex = count
                    compactReadBuffer()
                    return nil
                }
                let first = readBuffer[start]
                if first != 0x7B && first != 0x5B {
                    if let jsonStart = readBuffer[start...].firstIndex(where: { $0 == 0x7B || $0 == 0x5B }) {
                        logger.debug("Discarded \(jsonStart - start) non-JSON prefix bytes before JSON start")
                        readOffset = jsonStart
                        scanIndex = jsonStart
                        continue
                    }
                    if let newline = readBuffer[start...].firstIndex(of: 0x0A) {
                        logger.debug("Discarded non-JSON stdout line (\(newline - start) bytes)")
                        readOffset = newline + 1
                        scanIndex = readOffset
                        continue
                    }
                    if count - start > 4096 {
                        logger.warning("Discarding \(count - start) bytes of non-JSON stdout")
                        resetReadState()
                    }
                    return nil
                }
                readOffset = start
                scanIndex = start
            }

            // Inside a message, or at its first byte: walk on from the
            // persisted index with the persisted string and depth state.
            var end: Int?
            readBuffer.withUnsafeBytes { raw in
                let bytes = raw.bindMemory(to: UInt8.self)
                var index = scanIndex
                while index < count {
                    let byte = bytes[index]
                    if scanInString {
                        if scanEscaped {
                            scanEscaped = false
                        } else if byte == 0x5C {
                            scanEscaped = true
                        } else if byte == 0x22 {
                            scanInString = false
                        }
                    } else if byte == 0x22 {
                        scanInString = true
                    } else if byte == 0x7B || byte == 0x5B {
                        scanDepth += 1
                    } else if byte == 0x7D || byte == 0x5D {
                        scanDepth -= 1
                        if scanDepth <= 0 {
                            end = index
                            index += 1
                            break
                        }
                    }
                    index += 1
                }
                scanIndex = index
            }

            guard let end else {
                if count - readOffset > Self.largeBufferWarningThreshold {
                    logger.warning("Large buffer (\(count - self.readOffset) bytes) without complete JSON message")
                }
                return nil
            }
            let message = readBuffer.subdata(in: readOffset..<(end + 1))
            readOffset = end + 1
            scanIndex = readOffset
            scanDepth = 0
            scanInString = false
            scanEscaped = false
            compactReadBuffer()
            return message
        }
    }

    private static func isWhitespace(_ byte: UInt8) -> Bool {
        byte == 0x20 || byte == 0x09 || byte == 0x0D || byte == 0x0A
    }

    /// Drops the consumed prefix once it dominates the buffer — one memmove
    /// per half-buffer of traffic, amortised O(1) per byte — or the whole
    /// buffer once everything is consumed.
    private func compactReadBuffer() {
        guard readOffset > 0 else { return }
        if readOffset >= readBuffer.count {
            readBuffer.removeAll(keepingCapacity: true)
            readOffset = 0
            scanIndex = 0
        } else if readOffset >= 65536, readOffset * 2 >= readBuffer.count {
            readBuffer.removeSubrange(0..<readOffset)
            scanIndex -= readOffset
            readOffset = 0
        }
    }

    private func resetReadState() {
        readBuffer.removeAll()
        readOffset = 0
        scanIndex = 0
        scanDepth = 0
        scanInString = false
        scanEscaped = false
    }

    private func flushRemainingBufferIfNeeded() async {
        await drainBufferedMessages()

        let remaining = readBuffer.count > readOffset
            ? readBuffer.subdata(in: readOffset..<readBuffer.count)
            : Data()
        resetReadState()
        if !remaining.isEmpty {
            await onDataReceived?(remaining)
        }
    }

    private func waitForExit(_ proc: Process, timeout: TimeInterval) async -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while proc.isRunning, Date() < deadline {
            try? await Task.sleep(nanoseconds: 50_000_000)
        }
        return !proc.isRunning
    }
}
#endif
