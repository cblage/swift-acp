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

    private var process: ACPSpawnedProcess?
    private var processGroupId: pid_t?
    private var stdinPipe: Pipe?
    private var stdoutPipe: Pipe?
    private var stderrPipe: Pipe?
    private var stdinDescriptor: Int32 = -1
    /// The running process's stdout and stderr, read and framed on the
    /// reader's own serial queue — see `ACPOutputReader`.
    private var reader: ACPOutputReader?

    private let logger: Logger

    /// The handlers cross to the reader's queue and are installed by the
    /// client at its own construction, before any launch, so they live
    /// under a lock rather than on the actor.
    private let handlerLock = NSLock()
    nonisolated(unsafe) private var onMessage: (@Sendable (Data, ACPFrameHeader) -> Void)?
    nonisolated(unsafe) private var onStdoutLine: (@Sendable (String) -> Void)?
    nonisolated(unsafe) private var onTermination: (@Sendable (Int32) async -> Void)?

    /// Stdin writes run HERE, never on the actor: a write to a full pipe
    /// blocks until the agent reads, and an agent busy emitting a replay
    /// may not read for tens of seconds. On the actor that blocked
    /// `processOutput` too, so stdout went unprocessed for exactly as long
    /// — a 2026-09-03 profile showed model and effort requests parked
    /// 14–32s in the write while the reader waited on the actor and the
    /// transcript stalled. A serial queue keeps the writes ordered.
    /// THE INTAKE'S QoS COMES FROM THE APP at the client's construction
    /// (2026-09-06): the library bakes no band into its queues. This queue
    /// and the reader's are created at `qos`, and when one is given every
    /// write and every read handler ENFORCES it, so neither inherits the
    /// band its submitter happened to carry; `.unspecified` leaves plain
    /// queues and plain handlers.
    private let qos: DispatchQoS
    private let enforced: DispatchWorkItemFlags
    private let writeQueue: DispatchQueue

    /// The connection's queue, this actor's executor — see `Client`.
    private let executionQueue: DispatchSerialQueue

    nonisolated var unownedExecutor: UnownedSerialExecutor {
        executionQueue.asUnownedSerialExecutor()
    }

    private var stderrLineContinuation: AsyncStream<String>.Continuation?
    private var stderrLineStream: AsyncStream<String>?

    // MARK: - Initialization

    /// No coder of its own: the frames it reads leave as bytes with their
    /// header, and the messages it writes arrive as bytes the client
    /// encoded.
    init(
        qos: DispatchQoS,
        executor: DispatchSerialQueue = DispatchSerialQueue(label: "org.acp.process")
    ) {
        self.executionQueue = executor
        self.logger = Logger.forCategory("ACPProcessManager")
        self.qos = qos
        self.enforced = qos == .unspecified ? [] : [.enforceQoS]
        self.writeQueue = DispatchQueue(label: "org.acp.process.stdin", qos: qos)
    }

    // MARK: - Process Lifecycle

    func launch(agentPath: String, arguments: [String] = [], workingDirectory: String? = nil, environment customEnvironment: [String: String]? = nil) throws {
        guard process == nil else {
            throw ClientError.invalidResponse
        }

        let resolvedPath = (try? FileManager.default.destinationOfSymbolicLink(atPath: agentPath)) ?? agentPath
        let actualPath = resolvedPath.hasPrefix("/") ? resolvedPath : ((agentPath as NSString).deletingLastPathComponent as NSString).appendingPathComponent(resolvedPath)

        let isNodeScript: Bool = {
            guard let handle = FileHandle(forReadingAtPath: actualPath) else { return false }
            defer { try? handle.close() }
            guard let data = try? handle.read(upToCount: 64),
                  let firstLine = String(data: data, encoding: .utf8) else { return false }
            return firstLine.hasPrefix("#!/usr/bin/env node")
        }()

        let executable: String
        let argv: [String]
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
                executable = nodePath
                argv = [nodePath, actualPath] + arguments
            } else {
                executable = agentPath
                argv = [agentPath] + arguments
            }
        } else {
            executable = agentPath
            argv = [agentPath] + arguments
        }

        var environment = ShellEnvironment.loadUserShellEnvironment()

        // Merge custom environment variables (override shell env)
        if let customEnvironment {
            for (key, value) in customEnvironment {
                environment[key] = value
            }
        }

        var childDirectory: String?
        if let workingDirectory, !workingDirectory.isEmpty {
            environment["PWD"] = workingDirectory
            environment["OLDPWD"] = workingDirectory
            childDirectory = workingDirectory
        }

        let agentDir = (agentPath as NSString).deletingLastPathComponent

        if let existingPath = environment["PATH"] {
            environment["PATH"] = "\(agentDir):\(existingPath)"
        } else {
            environment["PATH"] = agentDir
        }

        let stdin = Pipe()
        let stdout = Pipe()
        let stderr = Pipe()

        let pid = try Self.spawn(
            executable, arguments: argv, environment: environment, workingDirectory: childDirectory,
            stdin: stdin.fileHandleForReading.fileDescriptor,
            stdout: stdout.fileHandleForWriting.fileDescriptor,
            stderr: stderr.fileHandleForWriting.fileDescriptor
        )
        // The child's ends are the child's now: closed here, so the reader
        // sees EOF when the agent exits and a write to a dead agent fails
        // instead of filling a pipe nobody reads.
        try? stdin.fileHandleForReading.close()
        try? stdout.fileHandleForWriting.close()
        try? stderr.fileHandleForWriting.close()

        stdinPipe = stdin
        stdoutPipe = stdout
        stderrPipe = stderr

        stdinDescriptor = stdin.fileHandleForWriting.fileDescriptor
        process = ACPSpawnedProcess(pid: pid) { [weak self] exitCode in
            Task {
                await self?.handleTermination(exitCode: exitCode)
            }
        }
        // The group is the spawn's: the child led it from its first
        // instruction, so there is nothing to set here and nothing to fail.
        processGroupId = pid
        Task {
            await ProcessRegistry.shared.recordProcess(pid: pid, pgid: pid, agentPath: actualPath)
        }

        var stderrContinuation: AsyncStream<String>.Continuation!
        stderrLineStream = AsyncStream { stderrContinuation = $0 }
        stderrLineContinuation = stderrContinuation
        let lines = stderrContinuation!

        handlerLock.lock()
        let onMessage = self.onMessage
        let onStdoutLine = self.onStdoutLine
        handlerLock.unlock()
        let reader = ACPOutputReader(
            stdout: stdout.fileHandleForReading,
            stderr: stderr.fileHandleForReading,
            logger: logger,
            qos: qos,
            onMessage: { data, header in onMessage?(data, header) },
            onStdoutLine: { line in onStdoutLine?(line) },
            onStderrLine: { line in lines.yield(line) }
        )
        self.reader = reader
        reader.start()
    }

    func isRunning() -> Bool {
        return process?.isRunning == true
    }

    func processIdentifier() -> Int32? {
        guard let process, process.isRunning, process.pid > 0 else {
            return nil
        }
        return process.pid
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
        let pid = proc?.pid

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
                _ = kill(proc.pid, SIGTERM)
            }
        }

        if let proc {
            let exited = await waitForExit(proc, timeout: 2.0)
            if !exited, proc.pid > 0 {
                if let pgid {
                    _ = killpg(pgid, SIGKILL)
                } else {
                    _ = kill(proc.pid, SIGKILL)
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

    /// Writes one encoded message as a line. The BYTES cross to this
    /// actor, encoded by the caller on its own: a generic `Encodable`
    /// value is not `Sendable`, and the client encodes once anyway.
    func writeMessage(_ data: Data) async throws {
        let fd = stdinDescriptor
        guard fd >= 0, let proc = process, proc.isRunning else {
            throw ClientError.processNotRunning
        }

        let lineData = data + Data([0x0A])

        // Only the descriptor crosses to the queue: the write loop is POSIX
        // so the closure captures nothing but Sendable values, and a
        // handle closed under a blocked write surfaces as an error here
        // instead of a hang.
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            // The app's band, enforced when one was given: the write holds
            // the intake's QoS whatever the submitter's.
            writeQueue.async(qos: qos, flags: enforced) {
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
    /// SYNCHRONOUSLY on the reader's queue with each complete frame and the
    /// header its walk read, in order; `onStdoutLine` on the same queue, in
    /// order with the frames around it, with every run of stdout bytes the
    /// framer set aside as not a frame — a line of text, a prefix before a
    /// frame, a line that opened like JSON and never closed — so an agent's
    /// own words on its JSON channel reach the client instead of the log
    /// alone; `onTermination` runs on this actor once every frame the pipes
    /// still held has been delivered.
    nonisolated func setHandlers(
        onMessage: @escaping @Sendable (Data, ACPFrameHeader) -> Void,
        onStdoutLine: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) async -> Void
    ) {
        handlerLock.lock()
        self.onMessage = onMessage
        self.onStdoutLine = onStdoutLine
        self.onTermination = onTermination
        handlerLock.unlock()
    }

    // MARK: - Private Methods

    private func handleTermination(exitCode: Int32) async {
        let pid = process?.pid
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
        // The scoped form: a bare lock/unlock pair is not allowed across
        // an async context, and nothing here awaits inside it.
        let onTermination = handlerLock.withLock { self.onTermination }
        await onTermination?(exitCode)
    }

    private func waitForExit(_ proc: ACPSpawnedProcess, timeout: TimeInterval) async -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while proc.isRunning, Date() < deadline {
            try? await Task.sleep(nanoseconds: 50_000_000)
        }
        return !proc.isRunning
    }

    /// THE SPAWN, `posix_spawn` with the child LEADING A PROCESS GROUP OF
    /// ITS OWN FROM ITS FIRST INSTRUCTION — `POSIX_SPAWN_SETPGROUP` with
    /// group 0, its pid — its stdio the three descriptors on 0, 1, and 2
    /// and every other descriptor closed to it (`POSIX_SPAWN_CLOEXEC_DEFAULT`),
    /// its signal mask empty and every disposition default, as Foundation's
    /// `Process` spawns, and its working directory changed in the child.
    /// The group is set at the spawn because `setpgid` from the parent
    /// afterwards is a race the parent loses: it fails with EPERM once the
    /// child has exec'd, and a child `posix_spawn` starts has exec'd before
    /// the parent's next line as a rule — so the group was mostly never set,
    /// and a terminate signalled the agent alone while the children it had
    /// spawned outlived it. `Process` exposes no spawn attribute, so the
    /// spawn is direct. Returns the pid; throws the spawn's errno.
    private nonisolated static func spawn(
        _ executable: String, arguments: [String], environment: [String: String],
        workingDirectory: String?, stdin: Int32, stdout: Int32, stderr: Int32
    ) throws -> pid_t {
        var attributes: posix_spawnattr_t? = nil
        guard posix_spawnattr_init(&attributes) == 0 else {
            throw ClientError.transportError("cannot launch \(executable): posix_spawnattr_init failed")
        }
        defer { posix_spawnattr_destroy(&attributes) }
        var noSignals = sigset_t()
        sigemptyset(&noSignals)
        var everySignal = sigset_t()
        sigfillset(&everySignal)
        posix_spawnattr_setsigmask(&attributes, &noSignals)
        posix_spawnattr_setsigdefault(&attributes, &everySignal)
        posix_spawnattr_setpgroup(&attributes, 0)
        posix_spawnattr_setflags(
            &attributes,
            Int16(POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_CLOEXEC_DEFAULT | POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF)
        )

        var actions: posix_spawn_file_actions_t? = nil
        guard posix_spawn_file_actions_init(&actions) == 0 else {
            throw ClientError.transportError("cannot launch \(executable): posix_spawn_file_actions_init failed")
        }
        defer { posix_spawn_file_actions_destroy(&actions) }
        posix_spawn_file_actions_adddup2(&actions, stdin, 0)
        posix_spawn_file_actions_adddup2(&actions, stdout, 1)
        posix_spawn_file_actions_adddup2(&actions, stderr, 2)
        if let workingDirectory {
            posix_spawn_file_actions_addchdir_np(&actions, workingDirectory)
        }

        let argv: [UnsafeMutablePointer<CChar>?] = arguments.map { strdup($0) } + [nil]
        let envp: [UnsafeMutablePointer<CChar>?] = environment.map { strdup("\($0.key)=\($0.value)") } + [nil]
        defer {
            for pointer in argv { free(pointer) }
            for pointer in envp { free(pointer) }
        }
        var pid: pid_t = 0
        let code = posix_spawn(&pid, executable, &actions, &attributes, argv, envp)
        guard code == 0 else {
            throw ClientError.transportError("cannot launch \(executable): \(String(cString: strerror(code)))")
        }
        return pid
    }
}

/// The agent's process as the spawn started it: its pid, which is also the
/// process group it leads, and its exit — observed by a process source that
/// reaps it and hands the status on ONCE, on the source's own queue, as
/// `Process.terminationStatus` would report it: the exit code of a process
/// that exited, the number of the signal that ended one.
final class ACPSpawnedProcess: @unchecked Sendable {
    let pid: pid_t
    private let lock = NSLock()
    private var exited = false
    private var source: DispatchSourceProcess?

    /// True until the exit has been reaped.
    var isRunning: Bool {
        lock.withLock { !exited }
    }

    init(pid: pid_t, onExit: @escaping @Sendable (Int32) -> Void) {
        self.pid = pid
        let source = DispatchSource.makeProcessSource(
            identifier: pid, eventMask: .exit, queue: DispatchQueue(label: "org.acp.process.exit")
        )
        self.source = source
        source.setEventHandler { [weak self] in
            self?.reap(onExit, blocking: true)
        }
        source.resume()
        // A child that exited between the spawn and the source's arming is a
        // zombie the source may never report: reaped here if it has, and the
        // source's own reap then finds nothing to take.
        reap(onExit, blocking: false)
    }

    private func reap(_ onExit: @Sendable (Int32) -> Void, blocking: Bool) {
        var raw: Int32 = 0
        guard waitpid(pid, &raw, blocking ? 0 : WNOHANG) == pid else { return }
        let signal = raw & 0x7f
        let status = signal == 0 ? (raw >> 8) & 0xff : signal
        lock.lock()
        let first = !exited
        exited = true
        lock.unlock()
        guard first else { return }
        source?.cancel()
        onExit(status)
    }
}

/// What the framer read off a frame's top level while walking it: the
/// `method` string, and whether an `id` key holds a value — `null` reads as
/// none, JSON-RPC's own rule for a notification. A notification is a frame
/// with a method and no id. Nil `method` for an array frame, a frame
/// without one, or an unframed tail at EOF, all of which the client's full
/// decode judges.
struct ACPFrameHeader: Sendable {
    let method: String?
    let hasId: Bool

    static let unknown = ACPFrameHeader(method: nil, hasId: false)
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
///
/// EVERY FRAME LEAVES WITH ITS HEADER: the walk that frames it reads the
/// top-level `method` and whether a top-level `id` holds a value as they
/// pass, so the client classifies a notification from the header instead
/// of decoding the frame for two keys — a full parse of a multi-megabyte
/// update that bought nothing, since the consumer parses the same bytes
/// once more for its typed payload.
final class ACPOutputReader: @unchecked Sendable {
    /// Created at the APP'S band (`qos`, from the client's construction —
    /// see `ACPProcessManager.qos`); the read handlers enforce it when one
    /// was given, so a frame never runs at whatever the source happened to
    /// inherit. The library bakes no band of its own.
    private let queue: DispatchQueue
    private let qos: DispatchQoS
    private let enforced: DispatchWorkItemFlags
    private let stdout: FileHandle
    private let stderr: FileHandle
    private let logger: Logger
    private let onMessage: @Sendable (Data, ACPFrameHeader) -> Void
    /// What the framer sets aside as not a frame, as text — see `setAside`.
    private let onStdoutLine: @Sendable (String) -> Void
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
    /// The header read as the walk passes the frame's top level: where the
    /// walk stands among a depth-one key, its colon, and its value; the
    /// key or `method` value being gathered; whether the key just read was
    /// `method` or `id`; and what the two keys held. Kept across chunks
    /// like the scan state, and reset at every frame boundary.
    private var headerPhase = HeaderPhase.none
    private var headerText: [UInt8] = []
    private var headerKeyIsMethod = false
    private var headerKeyIsId = false
    private var headerMethod: String?
    private var headerHasId = false
    private var stderrBuffer = Data()

    /// The walk's place inside the frame's top-level object. `none` is an
    /// array frame or no frame, where nothing is gathered.
    private enum HeaderPhase: UInt8 {
        case none, key, keyString, afterKey, value, valueString, afterValue
    }

    private static let methodKey: [UInt8] = Array("method".utf8)
    private static let idKey: [UInt8] = Array("id".utf8)

    init(
        stdout: FileHandle,
        stderr: FileHandle,
        logger: Logger,
        qos: DispatchQoS,
        onMessage: @escaping @Sendable (Data, ACPFrameHeader) -> Void,
        onStdoutLine: @escaping @Sendable (String) -> Void,
        onStderrLine: @escaping @Sendable (String) -> Void
    ) {
        self.stdout = stdout
        self.stderr = stderr
        self.logger = logger
        self.qos = qos
        self.enforced = qos == .unspecified ? [] : [.enforceQoS]
        self.queue = DispatchQueue(label: "org.acp.process.read", qos: qos)
        self.onMessage = onMessage
        self.onStdoutLine = onStdoutLine
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
        // The app's band, enforced when one was given — see `qos`.
        source.setEventHandler(qos: qos, flags: enforced) { [weak self] in
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
        while let (message, header) = popNextMessage() {
            onMessage(message, header)
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
            // An unframed tail: its header was never completed, so it
            // goes out unknown and the client's full decode judges it.
            onMessage(remaining, .unknown)
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
    /// the decoder's job: a malformed frame fails there and is logged. The
    /// frame's HEADER comes out with it, read by the same walk at its top
    /// level — see `ACPFrameHeader`.
    private func popNextMessage() -> (Data, ACPFrameHeader)? {
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
                    // NOT A FRAME: a whole line of text when the line ends
                    // before any frame opens on it, else the prefix before
                    // the frame that opens on it — each set aside to
                    // `onStdoutLine`, never to the log alone.
                    let newline = readBuffer[start...].firstIndex(of: 0x0A)
                    let jsonStart = readBuffer[start...].firstIndex(where: { $0 == 0x7B || $0 == 0x5B })
                    if let newline, jsonStart.map({ newline < $0 }) ?? true {
                        logger.debug("Set aside non-JSON stdout line (\(newline - start) bytes)")
                        setAside(start..<newline)
                        readOffset = newline + 1
                        scanIndex = readOffset
                        continue
                    }
                    if let jsonStart {
                        logger.debug("Set aside \(jsonStart - start) non-JSON prefix bytes before JSON start")
                        setAside(start..<jsonStart)
                        readOffset = jsonStart
                        scanIndex = jsonStart
                        continue
                    }
                    if count - start > 4096 {
                        logger.warning("Setting aside \(count - start) bytes of non-JSON stdout")
                        setAside(start..<count)
                        resetReadState()
                    }
                    return nil
                }
                readOffset = start
                scanIndex = start
                resetHeaderState()
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
            var phase = headerPhase
            var text = headerText
            var keyIsMethod = headerKeyIsMethod
            var keyIsId = headerKeyIsId
            var method = headerMethod
            var hasId = headerHasId
            // Gathering a depth-one key, or the value under `method`; every
            // other string is walked and nothing else. Hoisted so a byte
            // inside a string costs one test.
            var capturing = phase == .keyString || (phase == .valueString && keyIsMethod)
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
                            if capturing { text.append(byte) }
                        } else if byte == 0x5C {
                            escaped = true
                            if capturing { text.append(byte) }
                        } else if byte == 0x22 {
                            inString = false
                            if phase == .keyString {
                                keyIsMethod = text == Self.methodKey
                                keyIsId = text == Self.idKey
                                phase = .afterKey
                            } else if phase == .valueString {
                                // A method with an escape in it (an encoder
                                // that writes `\/`) is left to the full
                                // decode, which unescapes; the raw bytes
                                // would name a method nothing matches.
                                if keyIsMethod, !text.contains(0x5C) {
                                    method = String(decoding: text, as: UTF8.self)
                                }
                                phase = .afterValue
                            }
                            capturing = false
                        } else if capturing {
                            text.append(byte)
                        }
                    } else if byte == 0x22 {
                        inString = true
                        if depth == 1 {
                            if phase == .key {
                                text.removeAll(keepingCapacity: true)
                                phase = .keyString
                                capturing = true
                            } else if phase == .value {
                                text.removeAll(keepingCapacity: true)
                                if keyIsId { hasId = true }
                                phase = .valueString
                                capturing = keyIsMethod
                            }
                        }
                    } else if byte == 0x7B || byte == 0x5B {
                        if depth == 0 {
                            // The frame opens: an object's top level is
                            // read, an array's is not.
                            phase = byte == 0x7B ? .key : .none
                        } else if depth == 1, phase == .value {
                            // A nested value under a top-level key: `id`
                            // holds something, and the walk resumes at the
                            // top level once the depth returns.
                            if keyIsId { hasId = true }
                            phase = .afterValue
                        }
                        depth += 1
                    } else if byte == 0x7D || byte == 0x5D {
                        depth -= 1
                        if depth <= 0 {
                            end = index
                            index += 1
                            break
                        }
                    } else if depth == 1 {
                        if byte == 0x3A {
                            if phase == .afterKey { phase = .value }
                        } else if byte == 0x2C {
                            phase = .key
                        } else if phase == .value, byte != 0x20, byte != 0x09, byte != 0x0D {
                            // A number or a literal: `id` holds a value
                            // unless the literal is null.
                            if keyIsId { hasId = byte != 0x6E }
                            phase = .afterValue
                        }
                    }
                    index += 1
                }
            }
            scanIndex = index
            scanDepth = depth
            scanInString = inString
            scanEscaped = escaped
            headerPhase = phase
            headerText = text
            headerKeyIsMethod = keyIsMethod
            headerKeyIsId = keyIsId
            headerMethod = method
            headerHasId = hasId

            if let malformedLineEnd {
                logger.warning("Set aside malformed JSON stdout line (\(malformedLineEnd - self.readOffset) bytes)")
                setAside(readOffset..<malformedLineEnd)
                readOffset = malformedLineEnd + 1
                scanIndex = readOffset
                scanDepth = 0
                scanInString = false
                scanEscaped = false
                resetHeaderState()
                compactReadBuffer()
                continue
            }

            // A frame still arriving is normal traffic: the scan resumes
            // where it stopped, so a large pending frame costs its append
            // and nothing else.
            guard let end else { return nil }
            let message = readBuffer.subdata(in: readOffset..<(end + 1))
            let header = ACPFrameHeader(method: headerMethod, hasId: headerHasId)
            readOffset = end + 1
            scanIndex = readOffset
            scanDepth = 0
            scanInString = false
            scanEscaped = false
            resetHeaderState()
            compactReadBuffer()
            return (message, header)
        }
    }

    /// Bytes the framer sets aside as not a frame go out as text to
    /// `onStdoutLine`, a trailing carriage return dropped as a stderr
    /// line's is, on the read queue in their place among the frames — an
    /// agent's own words on its JSON channel, a login URL to open or a
    /// diagnostic, are the client's to read.
    private func setAside(_ range: Range<Int>) {
        var bytes = readBuffer.subdata(in: range)
        if bytes.last == 0x0D { bytes.removeLast() }
        guard !bytes.isEmpty else { return }
        onStdoutLine(String(decoding: bytes, as: UTF8.self))
    }

    private func resetHeaderState() {
        headerPhase = .none
        headerText.removeAll(keepingCapacity: true)
        headerKeyIsMethod = false
        headerKeyIsId = false
        headerMethod = nil
        headerHasId = false
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
        resetHeaderState()
    }
}
#endif
