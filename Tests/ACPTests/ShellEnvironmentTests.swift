#if os(macOS)
import XCTest
@testable import ACP

/// The login shell's environment: an output larger than a pipe's buffer
/// read whole, a shell past its timeout ended with nothing answered, a
/// shell that reads stdin given none, and every caller during a load
/// joining it — a failed load answered with the process's own environment
/// and cached for nobody.
final class ShellEnvironmentTests: XCTestCase {
    override func tearDown() {
        ShellEnvironment.resetForTesting()
        super.tearDown()
    }

    func testAnEnvironmentLargerThanThePipesBufferIsReadWhole() {
        let script = """
            i=0; while [ $i -lt 3000 ]; do \
            echo "K$i=0123456789012345678901234567890123456789012345678901234567890123456789"; \
            i=$((i+1)); done
            """
        let environment = ShellEnvironment.run(shell: "/bin/sh", arguments: ["-c", script], timeout: 10)
        XCTAssertEqual(environment?.count, 3000)
        XCTAssertEqual(environment?["K2999"]?.count, 70)
    }

    func testAShellPastItsTimeoutIsEndedAndAnswersNothing() {
        let started = Date()
        let environment = ShellEnvironment.run(shell: "/bin/sh", arguments: ["-c", "sleep 30"], timeout: 0.5)
        XCTAssertNil(environment)
        XCTAssertLessThan(Date().timeIntervalSince(started), 5)
    }

    func testAShellReadingStdinGetsNoInput() {
        let environment = ShellEnvironment.run(
            shell: "/bin/sh", arguments: ["-c", "read line; echo READ=done"], timeout: 5)
        XCTAssertEqual(environment?["READ"], "done")
    }

    func testCallersJoinOneLoadAndAFailedLoadIsCachedForNobody() {
        let calls = Calls()
        ShellEnvironment.loader = {
            calls.increment()
            Thread.sleep(forTimeInterval: 0.3)
            return nil
        }
        let group = DispatchGroup()
        let answers = Answers()
        for _ in 0..<3 {
            group.enter()
            DispatchQueue.global().async {
                answers.append(ShellEnvironment.loadUserShellEnvironmentBlocking())
                group.leave()
            }
        }
        XCTAssertEqual(group.wait(timeout: .now() + 5), .success)
        XCTAssertEqual(calls.count, 1, "One load, joined by the rest")
        XCTAssertEqual(answers.all.count, 3)
        XCTAssertTrue(
            answers.all.allSatisfy { $0 == ProcessInfo.processInfo.environment },
            "A failed load answers with the process's own environment")
        XCTAssertFalse(ShellEnvironment.isLoaded)

        ShellEnvironment.loader = {
            calls.increment()
            return ["LOADED": "yes"]
        }
        // Off the main thread, as a launch reads it: the synchronous form
        // joins the load rather than answering at once.
        let offMain = Answers()
        group.enter()
        Thread.detachNewThread {
            offMain.append(ShellEnvironment.loadUserShellEnvironment())
            group.leave()
        }
        XCTAssertEqual(group.wait(timeout: .now() + 5), .success)
        XCTAssertEqual(offMain.all.first?["LOADED"], "yes", "A failed load is tried again")
        XCTAssertTrue(ShellEnvironment.isLoaded)
        XCTAssertEqual(ShellEnvironment.loadUserShellEnvironmentBlocking()["LOADED"], "yes")
        XCTAssertEqual(calls.count, 2, "The cached answer runs nothing")
    }
}

private final class Calls: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    func increment() {
        lock.lock()
        value += 1
        lock.unlock()
    }
    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}

private final class Answers: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [[String: String]] = []
    func append(_ answer: [String: String]) {
        lock.lock()
        values.append(answer)
        lock.unlock()
    }
    var all: [[String: String]] {
        lock.lock()
        defer { lock.unlock() }
        return values
    }
}
#endif
