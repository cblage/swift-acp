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

    private let logger: Logger

    /// The handlers cross to the reader's queue and are installed by the
    /// client at its own construction, before any launch, so they live
    /// under a lock rather than on the actor.
    private let handlerLock = NSLock()
    nonisolated(unsafe) private var onMessage: (@Sendable (Data, ACPFrameHeader) -> Void)?
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

    private var stderrLineContinuation: AsyncStream<String>.Continuation?
    private var stderrLineStream: AsyncStream<String>?

    // MARK: - Initialization

    /// No coder of its own: the frames it reads leave as bytes with their
    /// header, and the messages it writes arrive as bytes the client
    /// encoded.
    init(qos: DispatchQoS) {
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
            qos: qos,
            onMessage: { data, header in onMessage?(data, header) },
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
    /// header its walk read, in order; `onTermination` runs on this actor
    /// once every frame the pipes still held has been delivered.
    nonisolated func setHandlers(
        onMessage: @escaping @Sendable (Data, ACPFrameHeader) -> Void,
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
        // The scoped form: a bare lock/unlock pair is not allowed across
        // an async context, and nothing here awaits inside it.
        let onTermination = handlerLock.withLock { self.onTermination }
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
        onStderrLine: @escaping @Sendable (String) -> Void
    ) {
        self.stdout = stdout
        self.stderr = stderr
        self.logger = logger
        self.qos = qos
        self.enforced = qos == .unspecified ? [] : [.enforceQoS]
        self.queue = DispatchQueue(label: "org.acp.process.read", qos: qos)
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
                logger.warning("Discarded malformed JSON stdout line (\(malformedLineEnd - self.readOffset) bytes)")
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
