#if os(macOS)
import Foundation
import Darwin

/// THE AGENT'S STDIN, written on ONE serial queue in call order, its close
/// queued behind the writes before it — the descriptor read on the queue,
/// so no write ever meets a descriptor closed and reused under it. Writes
/// run HERE, never on an actor: a write to a full pipe blocks until the
/// agent reads, and an agent busy emitting a replay may not read for tens
/// of seconds — on the process actor that blocked stdout's processing for
/// exactly as long (a 2026-09-03 profile showed model and effort requests
/// parked 14–32s in the write while the transcript stalled). A write the
/// agent never reads ends with the agent: its death fails the write with
/// EPIPE, never a signal, and the close queued behind it runs then.
final class ACPStdinWriter: @unchecked Sendable {
    private let lock = NSLock()
    private var handle: FileHandle?
    private let queue: DispatchQueue
    private let qos: DispatchQoS
    private let enforced: DispatchWorkItemFlags

    /// THE APP'S BAND, enforced when one was given: the write holds the
    /// intake's QoS whatever the submitter's.
    init(_ handle: FileHandle, qos: DispatchQoS) {
        self.handle = handle
        self.qos = qos
        self.enforced = qos == .unspecified ? [] : [.enforceQoS]
        self.queue = DispatchQueue(label: "org.acp.process.stdin", qos: qos)
    }

    /// The bytes as they are, after every write before them; `completion`
    /// gets nil once written, or the failure.
    func write(_ data: Data, completion: @escaping @Sendable (Error?) -> Void) {
        queue.async(qos: qos, flags: enforced) { [self] in
            guard let fd = lock.withLock({ handle?.fileDescriptor }), fd >= 0 else {
                completion(ClientError.processNotRunning)
                return
            }
            // A raw write to a pipe whose reader has died raises SIGPIPE for
            // the whole process; the descriptor opts out so a dead agent
            // reads as EPIPE, the error path, not a kill.
            _ = fcntl(fd, F_SETNOSIGPIPE, 1)
            let failure: Int32 = data.withUnsafeBytes { raw in
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
            completion(
                failure == 0
                    ? nil
                    : ClientError.transportError(
                        "stdin write failed: \(String(cString: strerror(failure)))"))
        }
    }

    /// Stdin closed after every write before it — the agent's own end.
    func close() {
        queue.async(qos: qos, flags: enforced) { [self] in
            let closing = lock.withLock { () -> FileHandle? in
                defer { handle = nil }
                return handle
            }
            try? closing?.close()
        }
    }
}

/// One caller's wait, separate from the frame that the serial writer must finish.
/// The lock settles completion, cancellation, expiry and connection end exactly once.
final class ACPWriteWait: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Error>?
    private var result: Result<Void, Error>?
    private var expiry: DispatchWorkItem?

    func install(_ continuation: CheckedContinuation<Void, Error>, deadline: DispatchTime?) {
        let expiry = deadline.map { _ in
            DispatchWorkItem { [weak self] in self?.finish(ClientError.requestTimeout) }
        }
        let settled = lock.withLock { () -> Result<Void, Error>? in
            if let result { return result }
            self.continuation = continuation
            self.expiry = expiry
            return nil
        }
        if let settled {
            continuation.resume(with: settled)
        } else if let expiry, let deadline {
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: deadline, execute: expiry)
        }
    }

    func finish(_ error: Error?) {
        let result: Result<Void, Error> = error.map { .failure($0) } ?? .success(())
        let waiting = lock.withLock { () -> CheckedContinuation<Void, Error>? in
            guard self.result == nil else { return nil }
            self.result = result
            expiry?.cancel()
            expiry = nil
            defer { continuation = nil }
            return continuation
        }
        waiting?.resume(with: result)
    }
}
#endif
