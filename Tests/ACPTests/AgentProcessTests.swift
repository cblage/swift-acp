import Darwin
import XCTest
@testable import ACP

/// The one process layer: an `AgentProcess` spoken to raw — its stdout as
/// written, its stdin in order, its stderr's tail and exit — and the end
/// policy every client shares: a grace for the agent's own end at its
/// stdin's close, and its group signalled when it ends on its own.
final class AgentProcessTests: XCTestCase {
    private var tempDir: URL!

    override func setUp() async throws {
        try await super.setUp()
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    }

    override func tearDown() async throws {
        if let tempDir { try? FileManager.default.removeItem(at: tempDir) }
        try await super.tearDown()
    }

    private func agent(_ script: String) throws -> String {
        let path = tempDir.appendingPathComponent("agent-\(UUID().uuidString).sh").path
        try "#!/bin/bash\n\(script)\n".write(toFile: path, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: path)
        return path
    }

    /// Collects stdout and the end, for a test to wait on.
    private final class Sink: @unchecked Sendable {
        private let lock = NSLock()
        private var bytes = Data()
        private var status: Int32?

        func append(_ data: Data) { lock.withLock { bytes.append(data) } }
        func end(_ code: Int32) { lock.withLock { status = code } }
        var text: String { String(decoding: lock.withLock { bytes }, as: UTF8.self) }
        var ended: Int32? { lock.withLock { status } }
    }

    private func waitUntil(
        _ seconds: TimeInterval = 5, _ condition: @escaping () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(seconds)
        while !condition(), Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }

    func testStdoutComesRawStdinGoesInOrderAndTheExitKeepsItsStatusAndStderr() async throws {
        let path = try agent("""
            read line
            echo "got:$line"
            echo "oops" >&2
            exit 3
            """)
        let sink = Sink()
        let process = AgentProcess()
        try await process.launch(
            agentPath: path, onStdout: { sink.append($0) }, onEnd: { sink.end($0) })
        process.write(Data("hello\n".utf8)) { error in XCTAssertNil(error) }
        try await waitUntil { sink.ended != nil }
        XCTAssertEqual(sink.text, "got:hello\n", "Stdout as the agent wrote it, unframed")
        XCTAssertEqual(sink.ended, 3)
        XCTAssertEqual(process.stderrTail, "oops\n")
        XCTAssertEqual(process.exit, AgentExit(status: 3, stderrTail: "oops\n"))
    }

    func testALeaderThatEndsOnItsOwnTakesItsGroup() async throws {
        let path = try agent("""
            sleep 30 &
            echo $!
            exit 0
            """)
        let sink = Sink()
        let process = AgentProcess()
        try await process.launch(
            agentPath: path, onStdout: { sink.append($0) }, onEnd: { sink.end($0) })
        try await waitUntil { sink.ended != nil }
        let child = pid_t(sink.text.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0
        XCTAssertGreaterThan(child, 0)
        try await waitUntil { kill(child, 0) != 0 }
        XCTAssertNotEqual(kill(child, 0), 0, "The program the agent started ends with it")
    }

    func testTheGraceLetsTheAgentEndItselfAtItsStdinsClose() async throws {
        let path = try agent("""
            while read line; do :; done
            exit 0
            """)
        let sink = Sink()
        let process = AgentProcess()
        try await process.launch(
            agentPath: path, onStdout: { sink.append($0) }, onEnd: { sink.end($0) })
        await process.terminate(grace: 2)
        try await waitUntil { process.exit != nil }
        XCTAssertEqual(process.exit?.status, 0, "Ended on its own, never by a signal")
    }

    func testWithoutAGraceTheStopSignalsTheAgent() async throws {
        let path = try agent("""
            trap '' HUP
            while true; do sleep 1; done
            """)
        let sink = Sink()
        let process = AgentProcess()
        try await process.launch(
            agentPath: path, onStdout: { sink.append($0) }, onEnd: { sink.end($0) })
        await process.terminate()
        try await waitUntil { process.exit != nil }
        XCTAssertEqual(process.exit?.status, SIGTERM)
    }

    func testAClientKeepsItsAgentsExitForTheWordsOnItsEnd() async throws {
        let path = try agent("""
            echo "fatal: boom" >&2
            exit 2
            """)
        let client = Client()
        let closed = expectation(description: "closed")
        client.setNotificationHandler({ _ in }, onClosed: { closed.fulfill() })
        try await client.launch(agentPath: path)
        await fulfillment(of: [closed], timeout: 5)
        XCTAssertEqual(client.lastExit, AgentExit(status: 2, stderrTail: "fatal: boom\n"))
    }
}
