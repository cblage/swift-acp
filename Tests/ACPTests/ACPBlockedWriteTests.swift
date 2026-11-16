import Darwin
import Foundation
import XCTest
@testable import ACP

final class ACPBlockedWriteTests: XCTestCase {
    private final class Outcome: @unchecked Sendable {
        private let lock = NSLock()
        private var result: Result<Void, Error>?
        func finish(_ value: Result<Void, Error>) { lock.withLock { result = value } }
        var error: Error? {
            lock.withLock {
                if case .failure(let error) = result { return error }
                return nil
            }
        }
    }

    private struct Pending {
        let task: Task<Void, Never>
        let ended: XCTestExpectation
        let outcome: Outcome
    }

    private var clients: [Client] = []
    private var pending: [Pending] = []
    private var readHandles: [FileHandle] = []
    private var writeHandles: [FileHandle] = []
    private let payload = String(repeating: "x", count: 1 << 20)

    override func tearDown() async throws {
        for client in clients { await client.terminate() }
        for handle in readHandles { try? handle.close() }
        for handle in writeHandles { try? handle.close() }
        for request in pending { await request.task.value }
        try await super.tearDown()
    }

    private func start(_ operation: @escaping @Sendable () async throws -> Void) -> Pending {
        let ended = XCTestExpectation(description: "The caller returns")
        let outcome = Outcome()
        let task = Task {
            do {
                try await operation()
                outcome.finish(.success(()))
            } catch {
                outcome.finish(.failure(error))
            }
            ended.fulfill()
        }
        let request = Pending(task: task, ended: ended, outcome: outcome)
        pending.append(request)
        return request
    }

    private func request(_ client: Client, timeout: TimeInterval?) -> Pending {
        let text = payload
        return start {
            _ = try await client.sendRequest(method: "test/request", params: text, timeout: timeout)
        }
    }

    private func assertAbandoned(_ client: Client, id: Int = 2) async {
        let tracked = await client.tracksBlockedWriteTestRequest(.number(id))
        XCTAssertFalse(tracked, "An abandoned request leaves every response bookkeeping table")
    }

    private func silentClient() async throws -> Client {
        let client = Client()
        clients.append(client)
        let ready = expectation(description: "The stand-in is ready and reads no stdin")
        client.setStdoutLineHandler { if $0 == "ready" { ready.fulfill() } }
        try await client.launch(
            agentPath: "/bin/sh", arguments: ["-c", "printf 'ready\\n'; exec /bin/sleep 600"])
        await fulfillment(of: [ready], timeout: 5)
        let written = expectation(description: "The pipe-filling frame is submitted")
        client.setWireTap { direction, _ in
            if direction == .outgoing { written.fulfill() }
        }
        _ = request(client, timeout: nil)
        await fulfillment(of: [written], timeout: 5)
        client.setWireTap(nil)
        return client
    }

    private func attachedClient() async throws -> (Client, Pipe, Pipe) {
        let input = Pipe()
        let output = Pipe()
        let client = Client()
        clients.append(client)
        readHandles.append(input.fileHandleForReading)
        writeHandles.append(output.fileHandleForWriting)
        try await client.attach(
            reading: output.fileHandleForReading, writing: input.fileHandleForWriting)
        return (client, input, output)
    }

    private func waitForWrite(in pipe: Pipe) async {
        let fd = pipe.fileHandleForReading.fileDescriptor
        let writing = await withCheckedContinuation { continuation in
            DispatchQueue.global().async {
                var descriptor = pollfd(fd: fd, events: Int16(POLLIN), revents: 0)
                continuation.resume(returning: poll(&descriptor, 1, 5_000) == 1)
            }
        }
        XCTAssertTrue(writing, "The frame has entered the pipe and exceeds its buffer")
    }

    func testTimeoutIncludesABlockedWrite() async throws {
        let client = try await silentClient()
        let startTime = ProcessInfo.processInfo.systemUptime
        let call = request(client, timeout: 0.2)
        let result = await XCTWaiter.fulfillment(of: [call.ended], timeout: 1)
        XCTAssertEqual(result, .completed, "The timeout must release the caller before any read")
        if result == .completed {
            guard case ClientError.requestTimeout? = call.outcome.error else {
                return XCTFail("Expected requestTimeout, got \(String(describing: call.outcome.error))")
            }
            XCTAssertLessThan(ProcessInfo.processInfo.systemUptime - startTime, 1)
            await assertAbandoned(client)
        }
        XCTAssertNil(client.lastExit, "The deadline does not terminate or drain the agent")
        try await checkLateWriteAndAnswer()
    }

    private func checkLateWriteAndAnswer() async throws {
        let (client, input, output) = try await attachedClient()
        let call = request(client, timeout: 0.2)
        await waitForWrite(in: input)
        let result = await XCTWaiter.fulfillment(of: [call.ended], timeout: 1)
        XCTAssertEqual(result, .completed)
        guard result == .completed else { return }
        guard case ClientError.requestTimeout? = call.outcome.error else {
            return XCTFail("The blocked request did not time out")
        }
        await assertAbandoned(client, id: 1)
        let next = start {
            let response = try await client.sendRequest(method: "test/next", params: "next", timeout: 2)
            XCTAssertEqual(response.id, .number(2))
        }
        let drained = expectation(description: "Both complete frames remain in wire order")
        DispatchQueue.global().async {
            var bytes = Data()
            var lines = 0
            while lines < 2 {
                let part = input.fileHandleForReading.availableData
                if part.isEmpty { break }
                lines += part.filter { $0 == 0x0A }.count
                bytes.append(part)
            }
            let frames = bytes.split(separator: 0x0A).compactMap {
                try? JSONSerialization.jsonObject(with: Data($0)) as? [String: Any]
            }
            XCTAssertEqual(frames.compactMap { $0["id"] as? Int }, [1, 2])
            XCTAssertEqual((frames.first?["params"] as? String)?.count, 1 << 20)
            try? output.fileHandleForWriting.write(contentsOf: Data(
                (#"{"jsonrpc":"2.0","id":1,"result":{}}"# + "\n"
                    + #"{"jsonrpc":"2.0","id":2,"result":{}}"# + "\n").utf8))
            drained.fulfill()
        }
        await fulfillment(of: [drained, next.ended], timeout: 5)
        XCTAssertNil(next.outcome.error)
        await assertAbandoned(client, id: 1)
        await assertAbandoned(client, id: 2)
    }

    func testCancellationIncludesABlockedWriteWithALimit() async throws {
        try await cancelledRequest(timeout: 30)
    }

    func testCancellationIncludesABlockedWriteWithoutALimit() async throws {
        try await cancelledRequest(timeout: nil)
    }

    private func cancelledRequest(timeout: TimeInterval?) async throws {
        let client = try await silentClient()
        let submitted = expectation(description: "The next frame is submitted")
        client.setWireTap { direction, _ in
            if direction == .outgoing { submitted.fulfill() }
        }
        let call = request(client, timeout: timeout)
        await fulfillment(of: [submitted], timeout: 5)
        call.task.cancel()
        let result = await XCTWaiter.fulfillment(of: [call.ended], timeout: 1)
        XCTAssertEqual(result, .completed, "Cancellation must release a caller during its write")
        if result == .completed {
            XCTAssertTrue(call.outcome.error is CancellationError)
            await assertAbandoned(client)
        }
        XCTAssertNil(client.lastExit)
    }

    func testCancellationIncludesABlockedCancelNotification() async throws {
        let client = try await silentClient()
        let submitted = expectation(description: "The cancel frame is submitted")
        client.setWireTap { direction, _ in
            if direction == .outgoing { submitted.fulfill() }
        }
        let call = start { try await client.sendCancelNotification(sessionId: SessionId("s")) }
        await fulfillment(of: [submitted], timeout: 5)
        call.task.cancel()
        let result = await XCTWaiter.fulfillment(of: [call.ended], timeout: 1)
        XCTAssertEqual(result, .completed)
        if result == .completed { XCTAssertTrue(call.outcome.error is CancellationError) }
        XCTAssertNil(client.lastExit)
    }

    func testTerminateReleasesAnAttachedWriteBeforeThePeerReads() async throws {
        let (client, input, _) = try await attachedClient()
        let call = request(client, timeout: nil)
        await waitForWrite(in: input)
        await client.terminate()
        let result = await XCTWaiter.fulfillment(of: [call.ended], timeout: 1)
        XCTAssertEqual(result, .completed, "Closing the connection must release its write wait")
        if result == .completed {
            guard case ClientError.processNotRunning? = call.outcome.error else {
                return XCTFail("Expected processNotRunning, got \(String(describing: call.outcome.error))")
            }
        }
    }

    func testTerminateSignalsAProcessWithBlockedAndQueuedWrites() async throws {
        let client = try await silentClient()
        let submitted = expectation(description: "Both requests are queued behind the blocked write")
        submitted.expectedFulfillmentCount = 2
        client.setWireTap { direction, _ in
            if direction == .outgoing { submitted.fulfill() }
        }
        let first = request(client, timeout: nil)
        let second = request(client, timeout: 30)
        await fulfillment(of: [submitted], timeout: 5)
        let startTime = ProcessInfo.processInfo.systemUptime
        await client.terminate()
        await fulfillment(of: [first.ended, second.ended], timeout: 5)
        XCTAssertNotNil(first.outcome.error)
        XCTAssertNotNil(second.outcome.error)
        XCTAssertNotNil(client.lastExit)
        XCTAssertLessThan(ProcessInfo.processInfo.systemUptime - startTime, 5)
        for id in 1...3 { await assertAbandoned(client, id: id) }
    }

    func testAnOrdinaryRequestStillReadsItsAnswer() async throws {
        let client = Client()
        clients.append(client)
        try await client.launch(
            agentPath: "/bin/sh", arguments: ["-c", #"read -r line; printf '{"jsonrpc":"2.0","id":1,"result":{}}\n'; exec /bin/sleep 600"#])
        let response = try await client.sendRequest(method: "test/request", params: "small", timeout: 2)
        XCTAssertEqual(response.id, .number(1))
        XCTAssertNil(response.error)
    }

    func testAnAnswerDuringTheWriteIsKeptUntilTheWriteCompletes() async throws {
        let (client, input, output) = try await attachedClient()
        let marker = expectation(description: "The response passed the read queue")
        client.setNotificationHandler { _ in marker.fulfill() }
        let text = payload
        let call = start {
            let response = try await client.sendRequest(method: "test/request", params: text, timeout: 5)
            XCTAssertEqual(response.id, .number(1))
        }
        await waitForWrite(in: input)
        try output.fileHandleForWriting.write(contentsOf: Data(
            (#"{"jsonrpc":"2.0","id":1,"result":{}}"# + "\n"
                + #"{"jsonrpc":"2.0","method":"test/marker","params":{}}"# + "\n").utf8))
        await fulfillment(of: [marker], timeout: 5)
        let notYet = XCTestExpectation(description: "The write remains blocked")
        notYet.isInverted = true
        let observer = Task { await call.task.value; notYet.fulfill() }
        await fulfillment(of: [notYet], timeout: 0.05)
        let drained = expectation(description: "The complete frame reaches the peer")
        DispatchQueue.global().async {
            var bytes = Data()
            while bytes.last != 0x0A {
                let part = input.fileHandleForReading.availableData
                if part.isEmpty { break }
                bytes.append(part)
            }
            XCTAssertGreaterThan(bytes.count, 1 << 20)
            drained.fulfill()
        }
        await fulfillment(of: [drained, call.ended], timeout: 5)
        XCTAssertNil(call.outcome.error)
        await observer.value
    }
}

private extension Client {
    func tracksBlockedWriteTestRequest(_ id: RequestId) -> Bool {
        expectedRequestIds.contains(id) || earlyResponses[id] != nil
            || pendingRequests[id] != nil || pendingTimeouts[id] != nil
    }
}
