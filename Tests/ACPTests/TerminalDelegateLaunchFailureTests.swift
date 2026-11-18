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

    private final class RegistrationGate: @unchecked Sendable {
        private let lock = NSLock()
        private var remaining: Int
        private var continuation: CheckedContinuation<Void, Never>?
        init(count: Int) { self.remaining = count }
        func arrive() {
            let ready: CheckedContinuation<Void, Never>? = lock.withLock {
                remaining -= 1
                guard remaining == 0 else { return nil }
                defer { continuation = nil }
                return continuation
            }
            ready?.resume()
        }
        func wait() async {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let settled = lock.withLock { () -> Bool in
                    if remaining <= 0 { return true }
                    self.continuation = continuation
                    return false
                }
                if settled { continuation.resume() }
            }
        }
    }

    private final class ControlledProcess: Process, @unchecked Sendable {
        struct Counts: Equatable {
            let created: Int
            let live: Int
            let aliveWeak: Int
            let waits: Int
        }

        final class Harness: @unchecked Sendable {
            private struct Weak { weak var value: ControlledProcess? }
            private let lock = NSLock()
            private var specs: [(status: Int32, stdout: String, immediate: Bool)]
            private var created = 0
            private var live = 0
            private var waits = 0
            private var refs: [Weak] = []
            private weak var deferred: ControlledProcess?
            init(_ specs: [(status: Int32, stdout: String, immediate: Bool)]) { self.specs = specs }
            func makeNext() -> Process {
                let spec = lock.withLock { specs.removeFirst() }
                let process = ControlledProcess(spec.status, spec.stdout, spec.immediate, self)
                lock.withLock {
                    created += 1
                    live += 1
                    refs.append(Weak(value: process))
                    if !spec.immediate { deferred = process }
                }
                return process
            }
            func noteDeinit() { lock.withLock { live -= 1 } }
            func noteWait() { lock.withLock { waits += 1 } }
            func finishDeferred() {
                let target = lock.withLock { () -> ControlledProcess? in
                    defer { deferred = nil }
                    return deferred
                }
                target?.finish()
            }
            var counts: Counts {
                lock.withLock {
                    Counts(
                        created: created, live: live,
                        aliveWeak: refs.filter { $0.value != nil }.count, waits: waits)
                }
            }
        }

        private let lock = NSLock()
        private let statusValue: Int32
        private let stdoutData: Data
        private let immediate: Bool
        private let harness: Harness
        private var fixtureIsRunning = false
        private var outputStore: Any?
        private var errorStore: Any?
        private var handlerStore: (@Sendable (Process) -> Void)?

        init(_ status: Int32, _ stdout: String, _ immediate: Bool, _ harness: Harness) {
            self.statusValue = status
            self.stdoutData = Data(stdout.utf8)
            self.immediate = immediate
            self.harness = harness
            super.init()
        }
        deinit { harness.noteDeinit() }

        override var executableURL: URL? { get { nil } set {} }
        override var arguments: [String]? { get { nil } set {} }
        override var environment: [String: String]? { get { nil } set {} }
        override var currentDirectoryURL: URL? { get { nil } set {} }
        override var standardInput: Any? { get { nil } set {} }
        override var standardOutput: Any? {
            get { lock.withLock { outputStore } }
            set { lock.withLock { outputStore = newValue } }
        }
        override var standardError: Any? {
            get { lock.withLock { errorStore } }
            set { lock.withLock { errorStore = newValue } }
        }
        override var terminationHandler: (@Sendable (Process) -> Void)? {
            get { lock.withLock { handlerStore } }
            set { lock.withLock { handlerStore = newValue } }
        }
        override var isRunning: Bool { lock.withLock { fixtureIsRunning } }
        override var terminationStatus: Int32 { statusValue }
        override var terminationReason: Process.TerminationReason { .exit }
        override var processIdentifier: Int32 { 4242 }
        override func run() throws {
            lock.withLock { fixtureIsRunning = true }
            if immediate { finish() }
        }
        override func terminate() { finish() }
        override func interrupt() { finish() }
        override func waitUntilExit() { harness.noteWait() }

        func finish() {
            var outPipe: Pipe?
            var errPipe: Pipe?
            var handler: (@Sendable (Process) -> Void)?
            let shouldFinish = lock.withLock { () -> Bool in
                guard fixtureIsRunning else { return false }
                fixtureIsRunning = false
                outPipe = outputStore as? Pipe
                errPipe = errorStore as? Pipe
                outputStore = nil
                errorStore = nil
                handler = handlerStore
                return true
            }
            guard shouldFinish else { return }
            if !stdoutData.isEmpty { try? outPipe?.fileHandleForWriting.write(contentsOf: stdoutData) }
            try? outPipe?.fileHandleForWriting.close()
            try? errPipe?.fileHandleForWriting.close()
            autoreleasepool { handler?(self) }
        }
    }

    func testTerminalProcessExitDeliversOnceInBothOrders() async {
        let settledFirst = TerminalProcessExit()
        settledFirst.deliver(17)
        settledFirst.deliver(99)
        let settledValues = [await settledFirst.wait(), await settledFirst.wait()]
        XCTAssertEqual(settledValues, [17, 17])

        let waitedFirst = TerminalProcessExit()
        let registered = RegistrationGate(count: 2)
        let firstWaiter = Task { await waitedFirst.wait(onRegister: { registered.arrive() }) }
        let secondWaiter = Task { await waitedFirst.wait(onRegister: { registered.arrive() }) }
        await registered.wait()
        waitedFirst.deliver(23)
        waitedFirst.deliver(42)
        let waitedValues = [await firstWaiter.value, await secondWaiter.value, await waitedFirst.wait()]
        XCTAssertEqual(waitedValues, [23, 23, 23])
    }

    func testExitCollectionAvoidsWaitUntilExitPreservesCacheAndReleasesProcesses() async throws {
        let immediateCodes: [Int32] = [7, 19, 42]
        var specs = immediateCodes.map { (status: $0, stdout: "out-\($0)\n", immediate: true) }
        specs.append((status: 29, stdout: "out-29\n", immediate: false))
        let harness = ControlledProcess.Harness(specs)
        let delegate = TerminalDelegate(makeProcess: { harness.makeNext() })

        for (index, code) in immediateCodes.enumerated() {
            let created = try await delegate.handleTerminalCreate(
                command: "/bin/echo", sessionId: "s1", args: nil, cwd: nil, env: nil, outputByteLimit: nil)
            let activeDuring = await delegate.activeTerminalCount
            let runningDuring = await delegate.isRunning(terminalId: created.terminalId)
            XCTAssertEqual(activeDuring, 1)
            XCTAssertEqual(harness.counts.live, 1)
            XCTAssertFalse(runningDuring)

            let output = try await delegate.handleTerminalOutput(
                terminalId: created.terminalId, sessionId: "s1")
            let exit = try await delegate.handleTerminalWaitForExit(
                terminalId: created.terminalId, sessionId: "s1")
            XCTAssertEqual(output.output, "out-\(code)\n")
            XCTAssertEqual(output.exitStatus?.exitCode, Int(code))
            XCTAssertEqual(exit.exitCode, Int(code))

            try await delegate.handleTerminalRelease(terminalId: created.terminalId, sessionId: "s1")
            let cached = await delegate.getOutput(terminalId: created.terminalId)
            let activeAfter = await delegate.activeTerminalCount
            XCTAssertEqual(cached, "out-\(code)\n")
            XCTAssertEqual(activeAfter, 0)
            XCTAssertEqual(harness.counts, .init(created: index + 1, live: 0, aliveWeak: 0, waits: 0))
        }

        let deferred = try await delegate.handleTerminalCreate(
            command: "/bin/echo", sessionId: "s1", args: nil, cwd: nil, env: nil, outputByteLimit: nil)
        let runningBeforeFinish = await delegate.isRunning(terminalId: deferred.terminalId)
        let liveOutput = try await delegate.handleTerminalOutput(
            terminalId: deferred.terminalId, sessionId: "s1")
        XCTAssertTrue(runningBeforeFinish)
        XCTAssertNil(liveOutput.exitStatus)

        harness.finishDeferred()
        let runningAfterFinish = await delegate.isRunning(terminalId: deferred.terminalId)
        let endedWait = try await delegate.handleTerminalWaitForExit(
            terminalId: deferred.terminalId, sessionId: "s1")
        let endedOutput = try await delegate.handleTerminalOutput(
            terminalId: deferred.terminalId, sessionId: "s1")
        XCTAssertFalse(runningAfterFinish)
        XCTAssertEqual(endedWait.exitCode, 29)
        XCTAssertEqual(endedOutput.output, "out-29\n")
        XCTAssertEqual(endedOutput.exitStatus?.exitCode, 29)

        await delegate.cleanup()
        let cleared = await delegate.getOutput(terminalId: deferred.terminalId)
        let activeFinal = await delegate.activeTerminalCount
        XCTAssertNil(cleared)
        XCTAssertEqual(activeFinal, 0)
        XCTAssertEqual(harness.counts, .init(created: 4, live: 0, aliveWeak: 0, waits: 0))
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
