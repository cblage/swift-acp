//
//  TerminalDelegate.swift
//  ACP
//
//  Default terminal delegate implementation
//

#if os(macOS)
import Darwin
import Foundation
import ACPModel

/// Tracks state of a single terminal
private struct TerminalState: @unchecked Sendable {
    let process: Process
    /// BYTES, not a `String`: a chunk of output costs its own append. The
    /// string buffer this replaces counted its characters on every chunk,
    /// a walk of up to a megabyte per 64 KiB that pinned a thread for the
    /// length of a chatty command.
    var outputBuffer = Data()
    var outputByteLimit: Int?
    var lastReadIndex: Int = 0
    var isReleased: Bool = false
    var wasTruncated: Bool = false
    var exitWaiters: [CheckedContinuation<(exitCode: Int?, signal: String?), Never>] = []
    /// The read sources on the process's stdout and stderr, cancelled at
    /// release; their cancel handlers close the handles.
    var sources: [DispatchSourceRead] = []
}

/// Cached output for released terminals
private struct ReleasedTerminalOutput: Sendable {
    let output: String
    let exitCode: Int?
}

/// Actor responsible for handling terminal operations for agent sessions
public actor TerminalDelegate {

    // MARK: - Errors

    public enum TerminalError: LocalizedError, Sendable {
        case terminalNotFound(String)
        case terminalReleased(String)
        case executableNotFound(String)
        case commandParsingFailed(String)

        public var errorDescription: String? {
            switch self {
            case .terminalNotFound(let id):
                return "Terminal with ID '\(id)' not found"
            case .terminalReleased(let id):
                return "Terminal with ID '\(id)' has been released"
            case .executableNotFound(let path):
                return "Executable not found: '\(path)'"
            case .commandParsingFailed(let command):
                return "Failed to parse command string: '\(command)'"
            }
        }
    }

    // MARK: - Private Properties

    private var terminals: [String: TerminalState] = [:]
    private var releasedOutputs: [String: ReleasedTerminalOutput] = [:]
    private var releasedOutputOrder: [String] = []
    private let defaultOutputByteLimit = 1_000_000
    private let maxReleasedOutputEntries = 50

    /// THIS ACTOR'S OWN SERIAL QUEUE, its executor: its jobs — every
    /// request the agent makes of its terminals, and every chunk of output
    /// they produce — run on a GCD thread of this queue's, never on the
    /// process-wide cooperative pool, whose fixed handful of threads a few
    /// chatty commands across connections pinned for every other
    /// connection at once. The read sources below fire on the same queue,
    /// so a chunk lands in the buffer without a task hop.
    private let executionQueue: DispatchSerialQueue

    public nonisolated var unownedExecutor: UnownedSerialExecutor {
        executionQueue.asUnownedSerialExecutor()
    }

    // MARK: - Private Cleanup

    /// What the pipes still hold, then the sources cancelled — whose cancel
    /// handlers close the handles — so nothing reads a closed descriptor.
    private func cleanupProcessPipes(terminalId: String) {
        drainAvailableOutput(terminalId: terminalId)
        guard var state = terminals[terminalId] else { return }
        for source in state.sources { source.cancel() }
        state.sources.removeAll()
        terminals[terminalId] = state
    }

    // MARK: - Initialization

    /// `qos` is the queue's band — the connection's intake QoS is the
    /// natural choice, since the agent waits on these replies.
    public init(qos: DispatchQoS = .unspecified) {
        executionQueue = DispatchSerialQueue(label: "org.acp.terminal", qos: qos)
    }

    // MARK: - Terminal Operations

    /// Create a new terminal process
    public func handleTerminalCreate(
        command: String,
        sessionId: String,
        args: [String]?,
        cwd: String?,
        env: [EnvVariable]?,
        outputByteLimit: Int?
    ) async throws -> CreateTerminalResponse {
        var executablePath: String
        var finalArgs: [String]

        let shellOperators = ["|", "&&", "||", ";", ">", ">>", "<", "$(", "`", "&"]
        let needsShell = shellOperators.contains { command.contains($0) }

        if needsShell {
            executablePath = "/bin/sh"
            if let args = args, !args.isEmpty {
                finalArgs = ["-c", ([command] + args).joined(separator: " ")]
            } else {
                finalArgs = ["-c", command]
            }
        } else if args == nil || args?.isEmpty == true {
            if command.contains(" ") || command.contains("\"") {
                let (parsedExecutable, parsedArgs) = try parseCommandString(command)
                executablePath = try resolveExecutablePath(parsedExecutable)
                finalArgs = parsedArgs
            } else {
                executablePath = try resolveExecutablePath(command)
                finalArgs = []
            }
        } else {
            executablePath = try resolveExecutablePath(command)
            finalArgs = args ?? []
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: executablePath)
        process.arguments = finalArgs

        if let cwd = cwd {
            process.currentDirectoryURL = URL(fileURLWithPath: cwd)
        }

        var envDict = ShellEnvironment.loadUserShellEnvironment()
        if let envVars = env {
            for envVar in envVars {
                envDict[envVar.name] = envVar.value
            }
        }
        process.environment = envDict

        let outputPipe = Pipe()
        let errorPipe = Pipe()
        process.standardOutput = outputPipe
        process.standardError = errorPipe

        let terminalIdValue = UUID().uuidString
        let terminalId = TerminalId(terminalIdValue)

        var state = TerminalState(process: process, outputByteLimit: outputByteLimit ?? defaultOutputByteLimit)
        state.sources = [
            makeReadSource(outputPipe.fileHandleForReading, terminalId: terminalIdValue),
            makeReadSource(errorPipe.fileHandleForReading, terminalId: terminalIdValue),
        ]
        terminals[terminalIdValue] = state

        try process.run()
        return CreateTerminalResponse(terminalId: terminalId, _meta: nil)
    }

    /// A pipe read on THIS ACTOR'S QUEUE as it becomes readable, the shape
    /// the process reader has: a non-blocking descriptor, a drain to empty
    /// per event, the bytes appended in place. The source is made on the
    /// executor's queue, so its handler is on the actor by construction and
    /// says so; the readability handler and the task per chunk it replaces
    /// crossed a global queue and the cooperative pool for every 64 KiB.
    private func makeReadSource(_ handle: FileHandle, terminalId: String) -> DispatchSourceRead {
        let fd = handle.fileDescriptor
        _ = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK)
        let source = DispatchSource.makeReadSource(fileDescriptor: fd, queue: executionQueue)
        source.setEventHandler { [weak self] in
            guard let self else { return }
            self.assumeIsolated { delegate in
                if delegate.drain(fd: fd, into: terminalId) { source.cancel() }
            }
        }
        source.setCancelHandler { try? handle.close() }
        source.resume()
        return source
    }

    /// Reads until the pipe holds nothing more right now, or is at EOF —
    /// true at EOF. Non-blocking by the flag set at the source's creation.
    private func drain(fd: Int32, into terminalId: String) -> Bool {
        var chunk = [UInt8](repeating: 0, count: 65536)
        while true {
            let count = chunk.withUnsafeMutableBytes { raw -> Int in
                guard let base = raw.baseAddress else { return 0 }
                return Darwin.read(fd, base, raw.count)
            }
            if count > 0 {
                appendOutput(terminalId: terminalId, bytes: chunk[0..<count])
                continue
            }
            if count == 0 { return true }
            if errno == EINTR { continue }
            return false
        }
    }

    /// Get output from a terminal process
    public func handleTerminalOutput(terminalId: TerminalId, sessionId: String) async throws -> TerminalOutputResponse {
        guard var state = terminals[terminalId.value] else {
            throw TerminalError.terminalNotFound(terminalId.value)
        }

        guard !state.isReleased else {
            throw TerminalError.terminalReleased(terminalId.value)
        }

        drainAvailableOutput(terminalId: terminalId.value)
        state = terminals[terminalId.value] ?? state

        let exitStatus: TerminalExitStatus?
        if state.process.isRunning {
            exitStatus = nil
        } else {
            exitStatus = TerminalExitStatus(
                exitCode: Int(state.process.terminationStatus),
                signal: nil,
                _meta: nil
            )
        }

        let output = outputText(of: state)
        return TerminalOutputResponse(
            output: output.text,
            exitStatus: exitStatus,
            truncated: output.truncated,
            _meta: nil
        )
    }

    /// Wait for a terminal process to exit
    public func handleTerminalWaitForExit(terminalId: TerminalId, sessionId: String) async throws -> WaitForExitResponse {
        guard let state = terminals[terminalId.value] else {
            throw TerminalError.terminalNotFound(terminalId.value)
        }

        guard !state.isReleased else {
            throw TerminalError.terminalReleased(terminalId.value)
        }

        if !state.process.isRunning {
            return WaitForExitResponse(
                exitCode: Int(state.process.terminationStatus),
                signal: nil,
                _meta: nil
            )
        }

        let result = await withCheckedContinuation { continuation in
            var waiterState = state
            waiterState.exitWaiters.append(continuation)
            terminals[terminalId.value] = waiterState

            Task {
                await self.monitorProcessExit(terminalId: terminalId)
            }
        }

        return WaitForExitResponse(
            exitCode: result.exitCode,
            signal: result.signal,
            _meta: nil
        )
    }

    /// Kill a terminal process
    public func handleTerminalKill(terminalId: TerminalId, sessionId: String) async throws -> KillTerminalResponse {
        guard var state = terminals[terminalId.value] else {
            throw TerminalError.terminalNotFound(terminalId.value)
        }

        guard !state.isReleased else {
            throw TerminalError.terminalReleased(terminalId.value)
        }

        if state.process.isRunning {
            state.process.terminate()
            state.process.waitUntilExit()
        }

        let exitCode = Int(state.process.terminationStatus)
        for waiter in state.exitWaiters {
            waiter.resume(returning: (exitCode, nil))
        }
        state.exitWaiters.removeAll()
        terminals[terminalId.value] = state

        return KillTerminalResponse(_meta: nil)
    }

    /// Release a terminal process
    public func handleTerminalRelease(terminalId: TerminalId, sessionId: String) async throws -> ReleaseTerminalResponse {
        guard var state = terminals[terminalId.value] else {
            throw TerminalError.terminalNotFound(terminalId.value)
        }

        if state.process.isRunning {
            state.process.terminate()
            state.process.waitUntilExit()
        }

        cleanupProcessPipes(terminalId: terminalId.value)
        state = terminals[terminalId.value] ?? state

        let exitCode = Int(state.process.terminationStatus)
        for waiter in state.exitWaiters {
            waiter.resume(returning: (exitCode, nil))
        }

        cacheReleasedOutput(
            terminalId: terminalId.value,
            output: outputText(of: state).text,
            exitCode: exitCode
        )

        state.isReleased = true
        state.exitWaiters.removeAll()
        terminals.removeValue(forKey: terminalId.value)

        return ReleaseTerminalResponse(_meta: nil)
    }

    /// Clean up all terminals
    public func cleanup() async {
        for (terminalId, state) in terminals {
            if state.process.isRunning {
                state.process.terminate()
                state.process.waitUntilExit()
            }
            cleanupProcessPipes(terminalId: terminalId)
            let exitCode = Int(state.process.terminationStatus)
            for waiter in state.exitWaiters {
                waiter.resume(returning: (exitCode, nil))
            }
        }
        terminals.removeAll()
        releasedOutputs.removeAll()
        releasedOutputOrder.removeAll()
    }

    // MARK: - Public Helpers

    /// Get terminal output for display
    public func getOutput(terminalId: TerminalId) -> String? {
        if let state = terminals[terminalId.value] {
            drainAvailableOutput(terminalId: terminalId.value)
            return outputText(of: terminals[terminalId.value] ?? state).text
        }
        return releasedOutputs[terminalId.value]?.output
    }

    /// Check if terminal is still running
    public func isRunning(terminalId: TerminalId) -> Bool {
        return terminals[terminalId.value]?.process.isRunning ?? false
    }

    /// What the pipes hold right now, read on the spot — a reply carries
    /// the output up to the moment it was asked for, not up to the last
    /// readable event. THROUGH THE LIVE SOURCES' DESCRIPTORS, NEVER THE
    /// FILE HANDLES: a source at EOF cancels itself and its cancel
    /// handler closes the handle, and a closed `NSFileHandle`'s
    /// `fileDescriptor` does not answer -1 — it raises
    /// `NSFileHandleOperationException`, an Objective-C exception no
    /// Swift frame catches, worded with whatever `errno` holds (the
    /// previous pipe's EAGAIN). A cancelled source is at EOF and holds
    /// nothing more; the descriptors are non-blocking.
    private func drainAvailableOutput(terminalId: String) {
        guard let state = terminals[terminalId] else { return }
        for source in state.sources where !source.isCancelled {
            _ = drain(fd: Int32(source.handle), into: terminalId)
        }
    }

    // MARK: - Private Helpers

    /// Appends a chunk for its own cost. THE LIMIT IS ENFORCED AMORTISED:
    /// the buffer runs to twice the limit before one trim back to it, at a
    /// UTF-8 boundary, so a chunk never pays a walk of the whole buffer;
    /// what a reply shows is cut to the limit at read time.
    private func appendOutput(terminalId: String, bytes: ArraySlice<UInt8>) {
        guard var state = terminals[terminalId] else { return }

        state.outputBuffer.append(contentsOf: bytes)

        if let limit = state.outputByteLimit, state.outputBuffer.count > limit * 2 {
            let cut = Self.utf8Boundary(in: state.outputBuffer, at: state.outputBuffer.count - limit)
            state.outputBuffer.removeFirst(cut)
            state.wasTruncated = true
        }

        terminals[terminalId] = state
    }

    /// The buffer's last `limit` bytes as text, at a UTF-8 boundary, and
    /// whether anything was ever cut.
    private func outputText(of state: TerminalState) -> (text: String, truncated: Bool) {
        var bytes = state.outputBuffer
        var truncated = state.wasTruncated
        if let limit = state.outputByteLimit, bytes.count > limit {
            let cut = Self.utf8Boundary(in: bytes, at: bytes.count - limit)
            bytes = bytes.suffix(from: bytes.startIndex + cut)
            truncated = true
        }
        return (String(decoding: bytes, as: UTF8.self), truncated)
    }

    /// The first offset at or past `offset` that starts a UTF-8 scalar.
    private static func utf8Boundary(in data: Data, at offset: Int) -> Int {
        var cut = offset
        while cut < data.count, data[data.startIndex + cut] & 0xC0 == 0x80 { cut += 1 }
        return cut
    }

    private func cacheReleasedOutput(terminalId: String, output: String, exitCode: Int) {
        releasedOutputs[terminalId] = ReleasedTerminalOutput(output: output, exitCode: exitCode)
        releasedOutputOrder.removeAll { $0 == terminalId }
        releasedOutputOrder.append(terminalId)

        while releasedOutputOrder.count > maxReleasedOutputEntries,
              let oldest = releasedOutputOrder.first {
            releasedOutputOrder.removeFirst()
            releasedOutputs.removeValue(forKey: oldest)
        }
    }

    private func monitorProcessExit(terminalId: TerminalId) async {
        guard let state = terminals[terminalId.value] else { return }
        let process = state.process

        while process.isRunning {
            try? await Task.sleep(nanoseconds: 100_000_000)
            guard terminals[terminalId.value] != nil else { return }
        }

        guard var currentState = terminals[terminalId.value],
              !currentState.exitWaiters.isEmpty else { return }

        let exitCode = Int(process.terminationStatus)
        for waiter in currentState.exitWaiters {
            waiter.resume(returning: (exitCode, nil))
        }
        currentState.exitWaiters.removeAll()
        terminals[terminalId.value] = currentState
    }

    private func parseCommandString(_ command: String) throws -> (String, [String]) {
        var executable: String?
        var args: [String] = []
        var currentArg = ""
        var inQuotes = false
        var escapeNext = false

        for char in command {
            if escapeNext {
                currentArg.append(char)
                escapeNext = false
                continue
            }

            if char == "\\" {
                escapeNext = true
                continue
            }

            if char == "\"" {
                inQuotes = !inQuotes
                continue
            }

            if char == " " && !inQuotes {
                if !currentArg.isEmpty {
                    if executable == nil {
                        executable = currentArg
                    } else {
                        args.append(currentArg)
                    }
                    currentArg = ""
                }
                continue
            }

            currentArg.append(char)
        }

        if !currentArg.isEmpty {
            if executable == nil {
                executable = currentArg
            } else {
                args.append(currentArg)
            }
        }

        guard let exec = executable, !exec.isEmpty else {
            throw TerminalError.commandParsingFailed(command)
        }

        return (exec, args)
    }

    private func resolveExecutablePath(_ command: String) throws -> String {
        let fileManager = FileManager.default

        if command.hasPrefix("/") {
            if fileManager.fileExists(atPath: command) {
                return command
            }
            throw TerminalError.executableNotFound(command)
        }

        let commonPaths = [
            "/usr/local/bin/\(command)",
            "/usr/bin/\(command)",
            "/bin/\(command)",
            "/opt/homebrew/bin/\(command)",
            "/opt/local/bin/\(command)",
        ]

        for path in commonPaths {
            if fileManager.fileExists(atPath: path) {
                return path
            }
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/which")
        process.arguments = [command]

        let pipe = Pipe()
        process.standardOutput = pipe

        defer {
            try? pipe.fileHandleForReading.close()
        }

        do {
            try process.run()
            process.waitUntilExit()
            if let data = try? pipe.fileHandleForReading.read(upToCount: 4096),
               let path = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) {
                if !path.isEmpty && FileManager.default.fileExists(atPath: path) {
                    return path
                }
            }
        } catch {
            // Fallback if 'which' fails
        }

        throw TerminalError.executableNotFound(command)
    }
}

// MARK: - Typealiases for backward compatibility

@available(*, deprecated, renamed: "TerminalDelegate")
public typealias ACPTerminalDelegate = TerminalDelegate
#endif
