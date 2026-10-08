import XCTest
@testable import ACP
import ACPModel

/// The client over an agent the test plays on two pipes: a notification
/// written to the agent's stdout reaches the handler and the wire tap, a
/// request the client sends leaves on the agent's stdin and the tap, the
/// answer written back completes it, and EOF on stdout closes the client.
final class ClientAttachTests: XCTestCase {
    /// The tap's record, under a lock: the taps land on two queues.
    private final class Taps: @unchecked Sendable {
        private let lock = NSLock()
        private var frames: [(DebugMessageDirection, Data)] = []
        func record(_ direction: DebugMessageDirection, _ data: Data) {
            lock.withLock { frames.append((direction, data)) }
        }
        var directions: [DebugMessageDirection] { lock.withLock { frames.map(\.0) } }
        var methods: [String?] {
            lock.withLock {
                frames.map { _, data in
                    (try? JSONSerialization.jsonObject(with: data) as? [String: Any])?["method"] as? String
                }
            }
        }
    }

    func testAnAttachedClientReadsWritesTapsAndClosesAtEOF() async throws {
        let agentOut = Pipe()
        let agentIn = Pipe()
        let client = Client()
        let taps = Taps()
        client.setWireTap { direction, data in taps.record(direction, data) }
        let notified = expectation(description: "the notification reached the handler")
        let closed = expectation(description: "the client closed at EOF")
        client.setNotificationHandler(
            { notification in
                XCTAssertEqual(notification.method, "session/update")
                notified.fulfill()
            },
            onClosed: { closed.fulfill() }
        )
        try await client.attach(
            reading: agentOut.fileHandleForReading, writing: agentIn.fileHandleForWriting)

        let update =
            #"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hi"}}}}"#
        agentOut.fileHandleForWriting.write(Data((update + "\n").utf8))
        await fulfillment(of: [notified], timeout: 5)

        // The agent's side of the request: its line read off stdin on a
        // thread of its own, then answered under the client's id.
        let requestLine = Task.detached { agentIn.fileHandleForReading.availableData }
        async let response = client.initialize(
            capabilities: ClientCapabilities(
                fs: FileSystemCapabilities(readTextFile: false, writeTextFile: false),
                terminal: false),
            timeout: 5)
        let request = await requestLine.value
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: request) as? [String: Any])
        XCTAssertEqual(object["method"] as? String, "initialize")
        let id = try XCTUnwrap(object["id"])
        let reply = #"{"jsonrpc":"2.0","id":\#(id),"result":{"protocolVersion":1,"agentCapabilities":{}}}"#
        agentOut.fileHandleForWriting.write(Data((reply + "\n").utf8))
        let initialized = try await response
        XCTAssertEqual(initialized.protocolVersion, 1)

        XCTAssertEqual(taps.directions, [.incoming, .outgoing, .incoming])
        XCTAssertEqual(taps.methods, ["session/update", "initialize", nil])

        try agentOut.fileHandleForWriting.close()
        await fulfillment(of: [closed], timeout: 5)
        await client.terminate()
    }

    /// A delegate set on a connection already reading answers the request
    /// the agent sends the moment `setDelegate` returns.
    func testADelegateSetOnALiveConnectionAnswersTheNextRequest() async throws {
        let agentOut = Pipe()
        let agentIn = Pipe()
        let client = Client()
        try await client.attach(
            reading: agentOut.fileHandleForReading, writing: agentIn.fileHandleForWriting)
        let delegate = MockClientDelegate()
        await client.setDelegate(delegate)

        let responseLine = Task.detached { agentIn.fileHandleForReading.availableData }
        let request =
            #"{"jsonrpc":"2.0","id":3,"method":"fs/read_text_file","params":{"sessionId":"s","path":"/notes.txt"}}"#
        agentOut.fileHandleForWriting.write(Data((request + "\n").utf8))
        let response = await responseLine.value
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: response) as? [String: Any])
        XCTAssertEqual(object["id"] as? Int, 3)
        XCTAssertNil(object["error"])
        let result = try XCTUnwrap(object["result"] as? [String: Any])
        XCTAssertEqual(result["content"] as? String, "mock content")

        try agentOut.fileHandleForWriting.close()
        await client.terminate()
        withExtendedLifetime(delegate) {}
    }
}
