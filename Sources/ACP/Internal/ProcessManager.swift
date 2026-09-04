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
    private var stdinDescriptor: Int32 = -1
    /// The running process's stdout and stderr, read and framed on the
    /// reader's own serial queue — see `ACPOutputReader`.
    private var reader: ACPOutputReader?

    private let encoder: JSONEncoder
    private let decoder: JSONDecoder
    private let logger: Logger

    /// The handlers cross to the reader's queue and are installed by the
    /// client at its own construction, before any launch, so they live
    /// under a lock rather than on the actor.
    private let handlerLock = NSLock()
    nonisolated(unsafe) private var onMessage: (@Sendable (Data) -> Void)?
    nonisolated(unsafe) private var onTermination: (@Sendable (Int32) async -> Void)?

    /// Stdin writes run HERE, never on the actor: a write to a full pipe
    /// blocks until the agent reads, and an agent busy emitting a replay
    /// may not read for tens of seconds. On the actor that blocked
    /// `processOutput` too, so stdout went unprocessed for exactly as long
    /// — a 2026-09-03 profile showed model and effort requests parked
    /// 14–32s in the write while the reader waited on the actor and the
    /// transcript stalled. A serial queue keeps the writes ordered.
    private let writeQueue = DispatchQueue(label: "org.acp.process.stdin", qos: .utility)

    private var stderrLineContinuation: AsyncStream<String>.Continuation?
    private var stderrLineStream: AsyncStream<String>?

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
        stdinDescriptor = stdin.fileHandleForWriting.fileDescriptor
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

        var stderrContinuation: AsyncStream<String>.Continuation!
        stderrLineStream = AsyncStream { stderrContinuation = $0 }
        stderrLineContinuation = stderrContinuation
        let lines = stderrContinuation!

        handlerLock.lock()
        let onMessage = self.onMessage
        handlerLock.unlock()
        let reader = ACPOutputReader(
            stdout: stdout.fileHandleForReading,
            stderr: stderr.fileHandleForReading,
            logger: logger,
            onMessage: { data in onMessage?(data) },
            onStderrLine: { line in lines.yield(line) }
        )
        self.reader = reader
        reader.start()
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

        stdinDescriptor = -1
        try? stdinPipe?.fileHandleForWriting.close()

        // A terminate discards what the agent was still saying: the reader
        // stops without draining, and its sources close the read ends on
        // their own queue once they have cancelled — never here, since
        // closing a descriptor a source still monitors is undefined.
        if let reader {
            await reader.stop()
            self.reader = nil
        }
        stderrLineContinuation?.finish()
        stderrLineContinuation = nil
        stderrLineStream = nil

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
    }

    // MARK: - I/O Operations

    func writeMessage<T: Encodable>(_ message: T) async throws {
        let fd = stdinDescriptor
        guard fd >= 0, let proc = process, proc.isRunning else {
            throw ClientError.processNotRunning
        }

        let data = try encoder.encode(message)

        var lineData = data
        lineData.append(0x0A)

        // Only the descriptor crosses to the queue: the write loop is POSIX
        // so the closure captures nothing but Sendable values, and a
        // handle closed under a blocked write surfaces as an error here
        // instead of a hang.
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            // `.utility`, enforced: the write must not inherit the caller's band.
            writeQueue.async(qos: .utility, flags: .enforceQoS) {
                // A raw write to a pipe whose reader has died raises SIGPIPE
                // for the whole process; the descriptor opts out so a dead
                // agent reads as EPIPE, the error path, not a kill.
                _ = fcntl(fd, F_SETNOSIGPIPE, 1)
                let failure: Int32 = lineData.withUnsafeBytes { raw in
                    guard let base = raw.baseAddress else { return 0 }
                    var offset = 0
                    while offset < raw.count {
                        let written = Darwin.write(fd, base + offset, raw.count - offset)
                        if written < 0 {
                            if errno == EINTR { continue }
                            return errno
                        }
                        offset += written
                    }
                    return 0
                }
                if failure == 0 {
                    continuation.resume()
                } else {
                    continuation.resume(throwing: ClientError.transportError("stdin write failed: \(String(cString: strerror(failure)))"))
                }
            }
        }
    }

    // MARK: - Callbacks

    /// Installs the receive and termination handlers. `onMessage` is called
    /// SYNCHRONOUSLY on the reader's queue with each complete frame, in
    /// order; `onTermination` runs on this actor once every frame the pipes
    /// still held has been delivered.
    nonisolated func setHandlers(
        onMessage: @escaping @Sendable (Data) -> Void,
        onTermination: @escaping @Sendable (Int32) async -> Void
    ) {
        handlerLock.lock()
        self.onMessage = onMessage
        self.onTermination = onTermination
        handlerLock.unlock()
    }

    // MARK: - Private Methods

    private func handleTermination(exitCode: Int32) async {
        let pid = process?.processIdentifier
        let pgid = processGroupId
        // ORDERED AFTER THE LAST MESSAGE: the reader drains both pipes to
        // EOF on its own queue, delivering every frame still in them and
        // the framer's remainder, before this resumes — so the termination
        // callback below can never overtake output the agent wrote before
        // it exited.
        if let reader {
            await reader.finish()
            self.reader = nil
        }
        stderrLineContinuation?.finish()
        stderrLineContinuation = nil
        stderrLineStream = nil

        stdinDescriptor = -1
        try? stdinPipe?.fileHandleForWriting.close()
        stdinPipe = nil
        stdoutPipe = nil
        stderrPipe = nil
        process = nil
        processGroupId = nil

        logger.info("Agent process terminated with code: \(exitCode)")
        await ProcessRegistry.shared.removeProcess(pid: pid, pgid: pgid)
        handlerLock.lock()
        let onTermination = self.onTermination
        handlerLock.unlock()
        await onTermination?(exitCode)
    }

    private func waitForExit(_ proc: Process, timeout: TimeInterval) async -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while proc.isRunning, Date() < deadline {
            try? await Task.sleep(nanoseconds: 50_000_000)
        }
        return !proc.isRunning
    }
}

/// The agent's stdout and stderr, read and FRAMED on ONE serial queue with
/// no stream, no consumer task, and no actor hop per message (2026-09-04).
/// A dispatch read source per pipe fires on the queue, the framer walks the
/// bytes on that thread, and every complete frame goes to `onMessage`
/// SYNCHRONOUSLY, still on the queue. The path this replaces yielded each
/// chunk into an unbounded stream, resumed a consumer task, hopped onto the
/// process actor to frame, and hopped onto the client actor per message —
/// three cooperative-pool schedulings per notification, tens of thousands
/// per replay, competing with every other task the pool ran.
///
/// Queue-confined by construction: every stored property below is touched
/// only on `queue`. The pipes are NON-BLOCKING, so the final drain at
/// termination reads to EOF without ever parking the queue on a write end
/// a grandchild inherited, and the read handles close in the sources'
/// cancel handlers — the one point past which a source is done with its
/// descriptor — never from the actor.
final class ACPOutputReader: @unchecked Sendable {
    /// `.utility`, and enforced on the read handler: the intake must not
    /// contend with the frame.
    private let queue = DispatchQueue(label: "org.acp.process.read", qos: .utility)
    private let stdout: FileHandle
    private let stderr: FileHandle
    private let logger: Logger
    private let onMessage: @Sendable (Data) -> Void
    private let onStderrLine: @Sendable (String) -> Void

    // Confined to `queue` from here on.
    private var stdoutSource: DispatchSourceRead?
    private var stderrSource: DispatchSourceRead?
    private var stdoutOpen = true
    private var stderrOpen = true
    private var ended = false
    private var chunk = [UInt8](repeating: 0, count: 65536)

    private var readBuffer = Data()
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
    private var stderrBuffer = Data()

    private static let largeBufferWarningThreshold = 200000

    init(
        stdout: FileHandle,
        stderr: FileHandle,
        logger: Logger,
        onMessage: @escaping @Sendable (Data) -> Void,
        onStderrLine: @escaping @Sendable (String) -> Void
    ) {
        self.stdout = stdout
        self.stderr = stderr
        self.logger = logger
        self.onMessage = onMessage
        self.onStderrLine = onStderrLine
    }

    func start() {
        queue.async { [self] in
            stdoutSource = makeSource(stdout, isStdout: true)
            stderrSource = makeSource(stderr, isStdout: false)
        }
    }

    /// Termination: drains both pipes to EOF, delivers every frame still in
    /// them and the framer's remainder, cancels the sources, and resumes —
    /// all on the read queue, so a caller's own termination handling that
    /// follows this is ordered after the last message.
    func finish() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            queue.async { [self] in
                if !ended {
                    ended = true
                    _ = drain(isStdout: true)
                    _ = drain(isStdout: false)
                    flushRemainder()
                    cancelSources()
                }
                continuation.resume()
            }
        }
    }

    /// A terminate: stops reading without draining, the sources closing the
    /// read handles as they cancel.
    func stop() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            queue.async { [self] in
                if !ended {
                    ended = true
                    cancelSources()
                }
                continuation.resume()
            }
        }
    }

    private func makeSource(_ handle: FileHandle, isStdout: Bool) -> DispatchSourceRead {
        let fd = handle.fileDescriptor
        _ = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK)
        let source = DispatchSource.makeReadSource(fileDescriptor: fd, queue: queue)
        source.setEventHandler(qos: .utility, flags: .enforceQoS) { [weak self] in
            guard let self else { return }
            if self.drain(isStdout: isStdout) {
                // EOF: the writer is gone; nothing more will ever arrive.
                (isStdout ? self.stdoutSource : self.stderrSource)?.cancel()
            }
        }
        source.setCancelHandler { [weak self] in
            try? handle.close()
            guard let self else { return }
            if isStdout { self.stdoutOpen = false } else { self.stderrOpen = false }
        }
        source.resume()
        return source
    }

    private func cancelSources() {
        stdoutSource?.cancel()
        stderrSource?.cancel()
        stdoutSource = nil
        stderrSource = nil
    }

    /// Reads until the pipe holds nothing more right now, or is at EOF —
    /// true at EOF. Non-blocking by the flag set at the source's creation.
    private func drain(isStdout: Bool) -> Bool {
        guard isStdout ? stdoutOpen : stderrOpen else { return true }
        let fd = (isStdout ? stdout : stderr).fileDescriptor
        guard fd >= 0 else { return true }
        while true {
            let count = chunk.withUnsafeMutableBytes { raw -> Int in
                guard let base = raw.baseAddress else { return 0 }
                return Darwin.read(fd, base, raw.count)
            }
            if count > 0 {
                let data = chunk.withUnsafeBytes { raw in
                    Data(bytes: raw.baseAddress!, count: count)
                }
                if isStdout {
                    receiveStdout(data)
                } else {
                    receiveStderr(data)
                }
                continue
            }
            if count == 0 { return true }
            if errno == EINTR { continue }
            return false
        }
    }

    private func receiveStdout(_ data: Data) {
        readBuffer.append(data)
        while let message = popNextMessage() {
            onMessage(message)
        }
    }

    private func receiveStderr(_ data: Data) {
        stderrBuffer.append(data)
        while let newlineIndex = stderrBuffer.firstIndex(of: 0x0A) {
            var line = Data(stderrBuffer[..<newlineIndex])
            let removeCount = stderrBuffer.distance(from: stderrBuffer.startIndex, to: newlineIndex) + 1
            stderrBuffer.removeFirst(min(removeCount, stderrBuffer.count))
            if line.last == 0x0D {
                line.removeLast()
            }
            onStderrLine(String(decoding: line, as: UTF8.self))
        }
    }

    /// The bytes that never closed into a message go out as one final
    /// frame, so the client logs a malformed tail instead of dropping it
    /// silently; a partial stderr line goes out as a line.
    private func flushRemainder() {
        let remaining = readBuffer.count > readOffset
            ? readBuffer.subdata(in: readOffset..<readBuffer.count)
            : Data()
        resetReadState()
        if !remaining.isEmpty {
            onMessage(remaining)
        }
        if !stderrBuffer.isEmpty {
            onStderrLine(String(decoding: stderrBuffer, as: UTF8.self))
            stderrBuffer.removeAll(keepingCapacity: true)
        }
    }

    // MARK: - JSON Message Parsing

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
            var malformedLineEnd: Int?
            // The walk runs on LOCALS and writes the state back once: every
            // stored-property access is a dynamic exclusivity check, and the
            // first cut paid three or four of them per byte — slower per
            // byte than the buffer copy it replaced.
            var index = scanIndex
            var depth = scanDepth
            var inString = scanInString
            var escaped = scanEscaped
            readBuffer.withUnsafeBytes { raw in
                let bytes = raw.bindMemory(to: UInt8.self)
                while index < count {
                    let byte = bytes[index]
                    if byte == 0x0A {
                        // ACP stdio is newline-delimited JSON. A newline
                        // reached before the top-level value closes marks a
                        // malformed/noisy line, including diagnostics such
                        // as `opening config {`. Recover at that boundary so
                        // one unmatched delimiter cannot absorb every later
                        // response. This remains part of the same one-pass
                        // scan, so fragmented valid lines are never rescanned.
                        malformedLineEnd = index
                        index += 1
                        break
                    } else if inString {
                        if escaped {
                            escaped = false
                        } else if byte == 0x5C {
                            escaped = true
                        } else if byte == 0x22 {
                            inString = false
                        }
                    } else if byte == 0x22 {
                        inString = true
                    } else if byte == 0x7B || byte == 0x5B {
                        depth += 1
                    } else if byte == 0x7D || byte == 0x5D {
                        depth -= 1
                        if depth <= 0 {
                            end = index
                            index += 1
                            break
                        }
                    }
                    index += 1
                }
            }
            scanIndex = index
            scanDepth = depth
            scanInString = inString
            scanEscaped = escaped

            if let malformedLineEnd {
                logger.warning("Discarded malformed JSON stdout line (\(malformedLineEnd - self.readOffset) bytes)")
                readOffset = malformedLineEnd + 1
                scanIndex = readOffset
                scanDepth = 0
                scanInString = false
                scanEscaped = false
                compactReadBuffer()
                continue
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
}
#endif
