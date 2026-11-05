#if os(macOS)
import Darwin
import XCTest
@testable import ACP
import ACPModel

final class TerminalDelegateKillTests: XCTestCase {
    private enum TestError: Error, CustomStringConvertible {
        case timeout(UInt64)

        var description: String {
            switch self {
            case .timeout(let nanoseconds):
                return "Timed out after \(nanoseconds) nanoseconds"
            }
        }
    }

    private final class Completion<Value: Sendable>: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Value, Error>?

        init(_ continuation: CheckedContinuation<Value, Error>) {
            self.continuation = continuation
        }

        func complete(with result: Result<Value, Error>) {
            lock.lock()
            guard let continuation else {
                lock.unlock()
                return
            }
            self.continuation = nil
            lock.unlock()
            continuation.resume(with: result)
        }
    }

    func testKillAnswersAndEscalatesWithoutBlockingTerminalRequests() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let killedPIDFile = directory.appendingPathComponent("killed.pid")
        let createdPIDFile = directory.appendingPathComponent("created.pid")
        defer {
            for file in [killedPIDFile, createdPIDFile] {
                guard let data = try? Data(contentsOf: file),
                      let pid = pid_t(String(decoding: data, as: UTF8.self)
                        .trimmingCharacters(in: .whitespacesAndNewlines)) else { continue }
                _ = Darwin.kill(pid, SIGKILL)
            }
            try? FileManager.default.removeItem(at: directory)
        }

        let delegate = TerminalDelegate()
        let requestBound: UInt64 = 1_000_000_000
        let killed = try await bounded(requestBound) {
            try await delegate.handleTerminalCreate(
                command: "/bin/sh",
                sessionId: "s1",
                args: ["-c", #"trap "" TERM; printf "$$" > "$1"; exec sleep 30"#, "test", killedPIDFile.path],
                cwd: nil,
                env: nil,
                outputByteLimit: nil
            )
        }.terminalId
        _ = try await readPID(from: killedPIDFile)

        let earlyWaiter = Task.detached {
            try await delegate.handleTerminalWaitForExit(terminalId: killed, sessionId: "s1")
        }
        try await Task.sleep(nanoseconds: 100_000_000)

        try await bounded(requestBound) {
            try await delegate.handleTerminalKill(terminalId: killed, sessionId: "s1")
        }

        let created = try await bounded(requestBound) {
            try await delegate.handleTerminalCreate(
                command: "/bin/sh",
                sessionId: "s1",
                args: ["-c", #"printf "$$" > "$1"; exec sleep 30"#, "test", createdPIDFile.path],
                cwd: nil,
                env: nil,
                outputByteLimit: nil
            )
        }.terminalId
        _ = try await readPID(from: createdPIDFile)

        try await bounded(requestBound) {
            try await delegate.handleTerminalOutput(terminalId: killed, sessionId: "s1")
        }

        let lateWaiter = Task.detached {
            try await delegate.handleTerminalWaitForExit(terminalId: killed, sessionId: "s1")
        }
        let earlyExit = try await bounded(3_000_000_000) {
            try await earlyWaiter.value
        }
        let lateExit = try await bounded(requestBound) {
            try await lateWaiter.value
        }
        XCTAssertEqual(earlyExit.exitCode, Int(SIGKILL))
        XCTAssertEqual(lateExit.exitCode, Int(SIGKILL))
        XCTAssertNil(earlyExit.signal)
        XCTAssertNil(lateExit.signal)

        try await bounded(requestBound) {
            try await delegate.handleTerminalKill(terminalId: created, sessionId: "s1")
        }
        _ = try await bounded(requestBound) {
            try await delegate.handleTerminalWaitForExit(terminalId: created, sessionId: "s1")
        }
        try await bounded(requestBound) {
            try await delegate.handleTerminalRelease(terminalId: killed, sessionId: "s1")
        }
        try await bounded(requestBound) {
            try await delegate.handleTerminalRelease(terminalId: created, sessionId: "s1")
        }
        try FileManager.default.removeItem(at: killedPIDFile)
        try FileManager.default.removeItem(at: createdPIDFile)
    }

    private func readPID(from file: URL) async throws -> pid_t {
        try await bounded(2_000_000_000) {
            let deadline = Date().addingTimeInterval(2)
            while Date() < deadline {
                if let data = try? Data(contentsOf: file),
                   let pid = pid_t(String(decoding: data, as: UTF8.self)
                    .trimmingCharacters(in: .whitespacesAndNewlines)) {
                    return pid
                }
                try await Task.sleep(nanoseconds: 20_000_000)
            }
            throw TestError.timeout(2_000_000_000)
        }
    }

    @discardableResult
    private func bounded<Value: Sendable>(
        _ nanoseconds: UInt64,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        try await withCheckedThrowingContinuation { continuation in
            let completion = Completion(continuation)
            Task.detached {
                do {
                    let value = try await operation()
                    completion.complete(with: .success(value))
                } catch {
                    completion.complete(with: .failure(error))
                }
            }
            Task.detached {
                try? await Task.sleep(nanoseconds: nanoseconds)
                completion.complete(with: .failure(TestError.timeout(nanoseconds)))
            }
        }
    }
}
#endif
