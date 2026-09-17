import XCTest
@testable import ACP
import ACPModel

/// The registry and the file delegate run on serial queues of their own,
/// and a frame that must enter the client's actor — an agent's request, a
/// routed notification — enters it from the read queue through the
/// actor's own queue: on a client attached to two pipes the test plays the
/// agent on, the request is answered through the library's file delegate
/// and the notification reaches the delegate.
final class ActorQueueTests: XCTestCase {
    /// A delegate whose file reads and writes are the library's own file
    /// delegate and whose completed elicitation fulfills an expectation;
    /// nothing else is asked of it.
    private final class ForwardingDelegate: ClientDelegate, @unchecked Sendable {
        private let files = FileSystemDelegate()
        private let elicitationCompleted: XCTestExpectation

        init(elicitationCompleted: XCTestExpectation) {
            self.elicitationCompleted = elicitationCompleted
        }

        func handleFileReadRequest(_ path: String, sessionId: String, line: Int?, limit: Int?) async throws -> ReadTextFileResponse {
            try await files.handleFileReadRequest(path, sessionId: sessionId, line: line, limit: limit)
        }

        func handleFileWriteRequest(_ path: String, content: String, sessionId: String) async throws -> WriteTextFileResponse {
            try await files.handleFileWriteRequest(path, content: content, sessionId: sessionId)
        }

        func handleTerminalCreate(command: String, sessionId: String, args: [String]?, cwd: String?, env: [EnvVariable]?, outputByteLimit: Int?) async throws -> CreateTerminalResponse {
            throw ClientError.invalidResponse
        }

        func handleTerminalOutput(terminalId: TerminalId, sessionId: String) async throws -> TerminalOutputResponse {
            throw ClientError.invalidResponse
        }

        func handleTerminalWaitForExit(terminalId: TerminalId, sessionId: String) async throws -> WaitForExitResponse {
            throw ClientError.invalidResponse
        }

        func handleTerminalKill(terminalId: TerminalId, sessionId: String) async throws -> KillTerminalResponse {
            throw ClientError.invalidResponse
        }

        func handleTerminalRelease(terminalId: TerminalId, sessionId: String) async throws -> ReleaseTerminalResponse {
            throw ClientError.invalidResponse
        }

        func handlePermissionRequest(request: RequestPermissionRequest) async throws -> RequestPermissionResponse {
            throw ClientError.invalidResponse
        }

        func handleCompleteElicitation(_ notification: CompleteElicitationNotification) async throws {
            XCTAssertEqual(notification.elicitationId.value, "e1")
            elicitationCompleted.fulfill()
        }
    }

    private func temporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    #if os(macOS)
    func testTheRegistryRecordsAndRemovesAProcessThroughItsFile() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let registry = ProcessRegistry(registryDirectory: directory)
        let file = directory.appendingPathComponent("acp-processes.json")

        await registry.recordProcess(pid: 4242, pgid: 4242, agentPath: "/usr/bin/true")
        let recorded = try JSONDecoder().decode([ProcessRegistry.Entry].self, from: Data(contentsOf: file))
        XCTAssertEqual(recorded.map(\.pid), [4242])
        XCTAssertEqual(recorded.map(\.agentPath), ["/usr/bin/true"])

        await registry.removeProcess(pid: 4242, pgid: nil)
        let removed = try JSONDecoder().decode([ProcessRegistry.Entry].self, from: Data(contentsOf: file))
        XCTAssertEqual(removed, [])
    }

    /// A `sleep` the test owns, running for its length or already exited.
    private func spawnSleep(_ seconds: String) throws -> Process {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/bin/sleep")
        process.arguments = [seconds]
        try process.run()
        return process
    }

    /// The file holds the agents that run: an entry whose process exited
    /// and one older than the age go at the next load, whoever wrote them,
    /// and a record carries the recording process.
    func testTheRegistryKeepsLiveEntriesOnlyAndStampsTheRecordingProcess() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let file = directory.appendingPathComponent("acp-processes.json")
        let exited = try spawnSleep("0")
        exited.waitUntilExit()
        let running = try spawnSleep("30")
        defer { running.terminate() }
        let now = Date().timeIntervalSince1970
        let written = [
            ProcessRegistry.Entry(pid: exited.processIdentifier, pgid: nil, agentPath: "/bin/sleep", startedAt: now),
            ProcessRegistry.Entry(pid: getpid(), pgid: nil, agentPath: "/bin/sleep", startedAt: now - 8 * 24 * 60 * 60),
            ProcessRegistry.Entry(pid: getpid(), pgid: nil, agentPath: "/bin/sleep", startedAt: now),
        ]
        try JSONEncoder().encode(written).write(to: file)

        let registry = ProcessRegistry(registryDirectory: directory)
        await registry.recordProcess(pid: running.processIdentifier, pgid: nil, agentPath: "/bin/sleep")
        let kept = try JSONDecoder().decode([ProcessRegistry.Entry].self, from: Data(contentsOf: file))
        XCTAssertEqual(kept.map(\.pid), [getpid(), running.processIdentifier])
        XCTAssertEqual(kept.map(\.ownerPid), [nil, getpid()])
    }

    /// The cleanup ends an agent whose client is gone and leaves a running
    /// client's agent alone, without a look at its process.
    func testTheCleanupEndsAnOrphanAndKeepsARunningClientsAgent() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let file = directory.appendingPathComponent("acp-processes.json")
        let orphan = try spawnSleep("30")
        let owned = try spawnSleep("30")
        defer { owned.terminate() }
        let now = Date().timeIntervalSince1970
        let written = [
            ProcessRegistry.Entry(pid: orphan.processIdentifier, pgid: nil, agentPath: "/bin/sleep", startedAt: now),
            ProcessRegistry.Entry(
                pid: owned.processIdentifier, pgid: nil, agentPath: "/bin/sleep", startedAt: now, ownerPid: getpid()),
        ]
        try JSONEncoder().encode(written).write(to: file)

        let registry = ProcessRegistry(registryDirectory: directory)
        await registry.cleanupOrphanedProcesses()
        XCTAssertFalse(orphan.isRunning)
        XCTAssertTrue(owned.isRunning)
        let kept = try JSONDecoder().decode([ProcessRegistry.Entry].self, from: Data(contentsOf: file))
        XCTAssertEqual(kept.map(\.pid), [owned.processIdentifier])
    }
    #endif

    func testAnAgentRequestAndARoutedNotificationEnterTheActorFromTheReadQueue() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let file = directory.appendingPathComponent("notes.txt")
        try "one\ntwo\n".write(to: file, atomically: true, encoding: .utf8)

        let agentOut = Pipe()
        let agentIn = Pipe()
        let client = Client()
        let completed = expectation(description: "the routed notification reached the delegate")
        let delegate = ForwardingDelegate(elicitationCompleted: completed)
        await client.setDelegate(delegate)
        try await client.attach(
            reading: agentOut.fileHandleForReading, writing: agentIn.fileHandleForWriting)

        // The agent's request, answered on its stdin through the file
        // delegate: the response line read off a thread of its own.
        let responseLine = Task.detached { agentIn.fileHandleForReading.availableData }
        let request =
            #"{"jsonrpc":"2.0","id":7,"method":"fs/read_text_file","params":{"sessionId":"s","path":"\#(file.path)"}}"#
        agentOut.fileHandleForWriting.write(Data((request + "\n").utf8))
        let response = await responseLine.value
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: response) as? [String: Any])
        XCTAssertEqual(object["id"] as? Int, 7)
        let result = try XCTUnwrap(object["result"] as? [String: Any])
        XCTAssertEqual(result["content"] as? String, "one\ntwo\n")
        XCTAssertEqual(result["total_lines"] as? Int, 3)

        let notification =
            #"{"jsonrpc":"2.0","method":"elicitation/complete","params":{"elicitationId":"e1"}}"#
        agentOut.fileHandleForWriting.write(Data((notification + "\n").utf8))
        await fulfillment(of: [completed], timeout: 5)

        try agentOut.fileHandleForWriting.close()
        await client.terminate()
        withExtendedLifetime(delegate) {}
    }
}
