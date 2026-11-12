#if os(macOS)
import XCTest
@testable import ACP
import ACPModel

final class TerminalDelegateLaunchFailureTests: XCTestCase {
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

    func testACreateThatFailsToLaunchKeepsNoTerminalAndCleanupReturns() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let missing = directory.appendingPathComponent("missing", isDirectory: true)

        let delegate = TerminalDelegate()
        let requestBound: UInt64 = 1_000_000_000
        do {
            _ = try await bounded(requestBound) {
                try await delegate.handleTerminalCreate(
                    command: "/bin/echo",
                    sessionId: "s1",
                    args: nil,
                    cwd: missing.path,
                    env: nil,
                    outputByteLimit: nil
                )
            }
            XCTFail("a create whose working directory does not exist returned a terminal")
        } catch let error as TestError {
            throw error
        } catch {
            // The launch failed. The terminal must not remain for cleanup to read.
        }

        try await bounded(requestBound) {
            await delegate.cleanup()
        }

        let created = try await bounded(requestBound) {
            try await delegate.handleTerminalCreate(
                command: "/bin/echo",
                sessionId: "s1",
                args: nil,
                cwd: directory.path,
                env: nil,
                outputByteLimit: nil
            )
        }
        let exit = try await bounded(requestBound) {
            try await delegate.handleTerminalWaitForExit(
                terminalId: created.terminalId, sessionId: "s1")
        }
        XCTAssertEqual(exit.exitCode, 0)
        try await bounded(requestBound) {
            try await delegate.handleTerminalRelease(
                terminalId: created.terminalId, sessionId: "s1")
        }
    }

    func testExitStatusIsReadAtEveryRequestAfterTheCommandEnds() async throws {
        let delegate = TerminalDelegate()
        let requestBound: UInt64 = 1_000_000_000
        let created = try await bounded(requestBound) {
            try await delegate.handleTerminalCreate(
                command: "/bin/sh",
                sessionId: "s1",
                args: ["-c", "exit 3"],
                cwd: nil,
                env: nil,
                outputByteLimit: nil
            )
        }
        let exit = try await bounded(requestBound) {
            try await delegate.handleTerminalWaitForExit(
                terminalId: created.terminalId, sessionId: "s1")
        }
        XCTAssertEqual(exit.exitCode, 3)

        let output = try await bounded(requestBound) {
            try await delegate.handleTerminalOutput(
                terminalId: created.terminalId, sessionId: "s1")
        }
        XCTAssertEqual(output.exitStatus?.exitCode, 3)

        try await bounded(requestBound) {
            try await delegate.handleTerminalRelease(
                terminalId: created.terminalId, sessionId: "s1")
        }
        try await bounded(requestBound) {
            await delegate.cleanup()
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
