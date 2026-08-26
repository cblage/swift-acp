import XCTest
@testable import ACP

final class ACPAgentTests: XCTestCase {

    func testCloseSessionInvokesCancelBeforeClose() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)
        let delegate = RecordingAgentDelegate()
        await agent.setDelegate(delegate)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let request = JSONRPCRequest(
            id: .number(1),
            method: "session/close",
            params: AnyCodable(["sessionId": "session-123"])
        )
        let requestData = try JSONEncoder().encode(request)
        await transport.pushMessage(requestData)

        let responseData = try await transport.nextSentMessage()
        let response = try JSONDecoder().decode(JSONRPCResponse.self, from: responseData)
        let events = await delegate.recordedEvents()

        XCTAssertEqual(response.id, .number(1))
        XCTAssertNil(response.error)
        XCTAssertEqual(events, [
            "cancel:session-123",
            "close:session-123",
        ])

        await transport.finish()
        _ = await startTask.result
    }

    func testResumeSessionRoutesToDelegate() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)
        let delegate = RecordingAgentDelegate()
        await agent.setDelegate(delegate)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let request = JSONRPCRequest(
            id: .number(2),
            method: "session/resume",
            params: AnyCodable([
                "sessionId": "session-123",
                "cwd": "/tmp/project",
                "additionalDirectories": ["/tmp/shared"],
            ])
        )
        let requestData = try JSONEncoder().encode(request)
        await transport.pushMessage(requestData)

        let responseData = try await transport.nextSentMessage()
        let response = try JSONDecoder().decode(JSONRPCResponse.self, from: responseData)
        let events = await delegate.recordedEvents()
        let result = try XCTUnwrap(response.result)
        let resultData = try JSONEncoder().encode(result)
        let resumeResponse = try JSONDecoder().decode(ResumeSessionResponse.self, from: resultData)

        XCTAssertEqual(response.id, .number(2))
        XCTAssertNil(response.error)
        XCTAssertEqual(events, [
            "resume:session-123:/tmp/project:/tmp/shared",
        ])
        XCTAssertEqual(resumeResponse.modes?.currentModeId, "chat")

        await transport.finish()
        _ = await startTask.result
    }

    func testDeleteSessionRoutesToDelegate() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)
        let delegate = RecordingAgentDelegate()
        await agent.setDelegate(delegate)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let request = JSONRPCRequest(
            id: .number(3),
            method: "session/delete",
            params: AnyCodable(["sessionId": "session-123"])
        )
        let requestData = try JSONEncoder().encode(request)
        await transport.pushMessage(requestData)

        let responseData = try await transport.nextSentMessage()
        let response = try JSONDecoder().decode(JSONRPCResponse.self, from: responseData)
        let events = await delegate.recordedEvents()

        XCTAssertEqual(response.id, .number(3))
        XCTAssertNil(response.error)
        XCTAssertEqual(events, [
            "delete:session-123",
        ])

        await transport.finish()
        _ = await startTask.result
    }

    func testLogoutRoutesWithoutParamsToDelegate() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)
        let delegate = RecordingAgentDelegate()
        await agent.setDelegate(delegate)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let request = JSONRPCRequest(
            id: .number(4),
            method: "logout",
            params: nil
        )
        let requestData = try JSONEncoder().encode(request)
        await transport.pushMessage(requestData)

        let responseData = try await transport.nextSentMessage()
        let response = try JSONDecoder().decode(JSONRPCResponse.self, from: responseData)
        let events = await delegate.recordedEvents()

        XCTAssertEqual(response.id, .number(4))
        XCTAssertNil(response.error)
        XCTAssertEqual(events, ["logout"])

        let nullParamsRequest = JSONRPCRequest(
            id: .number(5),
            method: "logout",
            params: AnyCodable(NSNull())
        )
        let nullParamsRequestData = try JSONEncoder().encode(nullParamsRequest)
        await transport.pushMessage(nullParamsRequestData)

        let nullParamsResponseData = try await transport.nextSentMessage()
        let nullParamsResponse = try JSONDecoder().decode(JSONRPCResponse.self, from: nullParamsResponseData)
        let nullParamsEvents = await delegate.recordedEvents()

        XCTAssertEqual(nullParamsResponse.id, .number(5))
        XCTAssertNil(nullParamsResponse.error)
        XCTAssertEqual(nullParamsEvents, ["logout", "logout"])

        await transport.finish()
        _ = await startTask.result
    }

    func testCancelRequestNotificationRoutesToDelegate() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)
        let delegate = RecordingAgentDelegate()
        await agent.setDelegate(delegate)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let notification = JSONRPCNotification(
            method: "$/cancel_request",
            params: AnyCodable(["requestId": 99])
        )
        let notificationData = try JSONEncoder().encode(notification)
        await transport.pushMessage(notificationData)

        try await Task.sleep(nanoseconds: 100_000_000)
        let events = await delegate.recordedEvents()

        XCTAssertEqual(events, ["cancel-request:99"])

        await transport.finish()
        _ = await startTask.result
    }

    func testDraftAgentRequestsRouteToDelegate() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)
        let delegate = RecordingAgentDelegate()
        await agent.setDelegate(delegate)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let forkRequest = JSONRPCRequest(
            id: .number(6),
            method: "session/fork",
            params: AnyCodable([
                "sessionId": "session-123",
                "cwd": "/tmp/fork",
                "additionalDirectories": ["/tmp/shared"],
            ])
        )
        await transport.pushMessage(try JSONEncoder().encode(forkRequest))

        let forkResponseData = try await transport.nextSentMessage()
        let forkResponse = try JSONDecoder().decode(JSONRPCResponse.self, from: forkResponseData)
        let forkResultData = try JSONEncoder().encode(try XCTUnwrap(forkResponse.result))
        let forkResult = try JSONDecoder().decode(ForkSessionResponse.self, from: forkResultData)

        XCTAssertEqual(forkResponse.id, .number(6))
        XCTAssertNil(forkResponse.error)
        XCTAssertEqual(forkResult.sessionId.value, "session-forked")

        let providerRequest = JSONRPCRequest(
            id: .number(7),
            method: "providers/list",
            params: AnyCodable([String: String]())
        )
        await transport.pushMessage(try JSONEncoder().encode(providerRequest))

        let providerResponseData = try await transport.nextSentMessage()
        let providerResponse = try JSONDecoder().decode(JSONRPCResponse.self, from: providerResponseData)
        let providerResultData = try JSONEncoder().encode(try XCTUnwrap(providerResponse.result))
        let providerResult = try JSONDecoder().decode(ListProvidersResponse.self, from: providerResultData)

        XCTAssertEqual(providerResult.providers.first?.providerId.value, "anthropic")

        let nesRequest = JSONRPCRequest(
            id: .number(8),
            method: "nes/suggest",
            params: AnyCodable([
                "sessionId": "nes-1",
                "uri": "file:///tmp/main.swift",
                "version": 1,
                "position": ["line": 0, "character": 0],
                "triggerKind": "manual",
            ] as [String: Any])
        )
        await transport.pushMessage(try JSONEncoder().encode(nesRequest))

        let nesResponseData = try await transport.nextSentMessage()
        let nesResponse = try JSONDecoder().decode(JSONRPCResponse.self, from: nesResponseData)
        let nesResultData = try JSONEncoder().encode(try XCTUnwrap(nesResponse.result))
        let nesResult = try JSONDecoder().decode(SuggestNesResponse.self, from: nesResultData)

        XCTAssertEqual(nesResult.suggestions.first?.id, "sug-1")

        let mcpRequest = JSONRPCRequest(
            id: .number(9),
            method: "mcp/message",
            params: AnyCodable([
                "connectionId": "conn-1",
                "method": "tools/list",
            ])
        )
        await transport.pushMessage(try JSONEncoder().encode(mcpRequest))

        let mcpResponseData = try await transport.nextSentMessage()
        let mcpResponse = try JSONDecoder().decode(JSONRPCResponse.self, from: mcpResponseData)
        let mcpResult = try XCTUnwrap(mcpResponse.result?.value as? [String: Any])

        XCTAssertEqual(mcpResult["ok"] as? Bool, true)

        let events = await delegate.recordedEvents()
        XCTAssertTrue(events.contains("fork:session-123:/tmp/fork:/tmp/shared"))
        XCTAssertTrue(events.contains("providers-list"))
        XCTAssertTrue(events.contains("nes-suggest:nes-1:manual"))
        XCTAssertTrue(events.contains("mcp-message:conn-1:tools/list"))

        await transport.finish()
        _ = await startTask.result
    }

    func testDraftAgentNotificationsRouteToDelegate() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)
        let delegate = RecordingAgentDelegate()
        await agent.setDelegate(delegate)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let reject = JSONRPCNotification(
            method: "nes/reject",
            params: AnyCodable([
                "sessionId": "nes-1",
                "id": "sug-1",
                "reason": "ignored",
            ])
        )
        await transport.pushMessage(try JSONEncoder().encode(reject))

        let document = JSONRPCNotification(
            method: "document/didFocus",
            params: AnyCodable([
                "sessionId": "nes-1",
                "uri": "file:///tmp/main.swift",
                "version": 2,
                "position": ["line": 1, "character": 2],
                "visibleRange": [
                    "start": ["line": 0, "character": 0],
                    "end": ["line": 20, "character": 0],
                ],
            ] as [String: Any])
        )
        await transport.pushMessage(try JSONEncoder().encode(document))

        let mcp = JSONRPCNotification(
            method: "mcp/message",
            params: AnyCodable([
                "connectionId": "conn-1",
                "method": "notifications/tools/list_changed",
            ])
        )
        await transport.pushMessage(try JSONEncoder().encode(mcp))

        try await Task.sleep(nanoseconds: 100_000_000)
        let events = await delegate.recordedEvents()

        XCTAssertTrue(events.contains("nes-reject:nes-1:sug-1:ignored"))
        XCTAssertTrue(events.contains("document-focus:nes-1:file:///tmp/main.swift"))
        XCTAssertTrue(events.contains("mcp-notification:conn-1:notifications/tools/list_changed"))

        await transport.finish()
        _ = await startTask.result
    }

    func testAgentCanInitiateClientDraftRequests() async throws {
        let transport = TestTransport()
        let agent = Agent(transport: transport)

        let startTask = Task {
            await agent.start()
        }
        defer {
            startTask.cancel()
        }

        let connectTask = Task {
            try await agent.connectMcp(serverId: "server-1")
        }

        let connectRequestData = try await transport.nextSentMessage()
        let connectRequest = try JSONDecoder().decode(JSONRPCRequest.self, from: connectRequestData)
        XCTAssertEqual(connectRequest.method, "mcp/connect")

        await transport.pushMessage(try JSONEncoder().encode(JSONRPCResponse(
            id: connectRequest.id,
            result: AnyCodable(["connectionId": "conn-1"]),
            error: nil
        )))

        let connectResponse = try await connectTask.value
        XCTAssertEqual(connectResponse.connectionId.value, "conn-1")

        let elicitationTask = Task {
            try await agent.createElicitation(CreateElicitationRequest(
                mode: "form",
                message: "Need input",
                requestId: .number(99),
                requestedSchema: ElicitationSchema()
            ))
        }

        let elicitationRequestData = try await transport.nextSentMessage()
        let elicitationRequest = try JSONDecoder().decode(JSONRPCRequest.self, from: elicitationRequestData)
        XCTAssertEqual(elicitationRequest.method, "elicitation/create")

        await transport.pushMessage(try JSONEncoder().encode(JSONRPCResponse(
            id: elicitationRequest.id,
            result: AnyCodable(["action": "accept", "content": ["value": "ok"]]),
            error: nil
        )))

        let elicitationResponse = try await elicitationTask.value
        XCTAssertEqual(elicitationResponse.action, "accept")
        XCTAssertEqual(elicitationResponse.content?["value"]?.value as? String, "ok")

        await transport.finish()
        _ = await startTask.result
    }

    func testClientRequestRouterRoutesDraftMethods() async throws {
        let router = ACPRequestRouter(encoder: JSONEncoder(), decoder: JSONDecoder())
        let delegate = RecordingClientDelegate()
        await router.setDelegate(delegate)

        let connect = JSONRPCRequest(
            id: .number(1),
            method: "mcp/connect",
            params: AnyCodable(["serverId": "server-1"])
        )
        let connectResult = try await router.routeRequest(connect)
        let connectData = try JSONEncoder().encode(connectResult)
        let connectResponse = try JSONDecoder().decode(ConnectMcpResponse.self, from: connectData)

        XCTAssertEqual(connectResponse.connectionId.value, "conn-1")

        let elicitation = JSONRPCRequest(
            id: .number(2),
            method: "elicitation/create",
            params: AnyCodable([
                "mode": "url",
                "message": "Authorize",
                "requestId": 12,
                "elicitationId": "elicit-1",
                "url": "https://example.com",
            ] as [String: Any])
        )
        let elicitationResult = try await router.routeRequest(elicitation)
        let elicitationData = try JSONEncoder().encode(elicitationResult)
        let elicitationResponse = try JSONDecoder().decode(CreateElicitationResponse.self, from: elicitationData)

        XCTAssertEqual(elicitationResponse.action, "decline")

        let custom = JSONRPCRequest(
            id: .number(3),
            method: "vendor/custom",
            params: AnyCodable(["value": "input"])
        )
        let customResult = try await router.routeRequest(custom)
        XCTAssertEqual(customResult.value as? String, "custom-response")

        try await router.routeNotification(JSONRPCNotification(
            method: "mcp/message",
            params: AnyCodable(["connectionId": "conn-1", "method": "notifications/progress"])
        ))
        try await router.routeNotification(JSONRPCNotification(
            method: "elicitation/complete",
            params: AnyCodable(["elicitationId": "elicit-1"])
        ))

        let events = await delegate.recordedEvents()
        XCTAssertEqual(events, [
            "mcp-connect:server-1",
            "elicitation-create:url",
            "mcp-notification:conn-1:notifications/progress",
            "elicitation-complete:elicit-1",
        ])
    }
}

private actor TestTransport: Transport {
    private let messageContinuation: AsyncStream<Data>.Continuation
    nonisolated let messages: AsyncStream<Data>

    private var sentMessages: [Data] = []
    private var sentContinuation: CheckedContinuation<Data, Error>?
    private var connected = true

    init() {
        var continuation: AsyncStream<Data>.Continuation!
        self.messages = AsyncStream { streamContinuation in
            continuation = streamContinuation
        }
        self.messageContinuation = continuation
    }

    func send(_ data: Data) async throws {
        if let continuation = sentContinuation {
            sentContinuation = nil
            continuation.resume(returning: data)
        } else {
            sentMessages.append(data)
        }
    }

    func close() async {
        connected = false
        messageContinuation.finish()
    }

    var isConnected: Bool {
        get async { connected }
    }

    func pushMessage(_ data: Data) {
        messageContinuation.yield(data)
    }

    func finish() {
        connected = false
        messageContinuation.finish()
    }

    func nextSentMessage() async throws -> Data {
        if !sentMessages.isEmpty {
            return sentMessages.removeFirst()
        }

        return try await withCheckedThrowingContinuation { continuation in
            sentContinuation = continuation
        }
    }
}

private actor RecordingAgentDelegate: AgentDelegate {
    private var events: [String] = []

    func handleInitialize(_ request: InitializeRequest) async throws -> InitializeResponse {
        fatalError("Not used in this test")
    }

    func handleNewSession(_ request: NewSessionRequest) async throws -> NewSessionResponse {
        fatalError("Not used in this test")
    }

    func handlePrompt(_ request: SessionPromptRequest) async throws -> SessionPromptResponse {
        fatalError("Not used in this test")
    }

    func handleCancel(_ sessionId: SessionId) async throws {
        events.append("cancel:\(sessionId.value)")
    }

    func handleLoadSession(_ request: LoadSessionRequest) async throws -> LoadSessionResponse {
        fatalError("Not used in this test")
    }

    func handleResumeSession(_ request: ResumeSessionRequest) async throws -> ResumeSessionResponse {
        let additionalDirectories = request.additionalDirectories?.joined(separator: ",") ?? ""
        events.append("resume:\(request.sessionId.value):\(request.cwd):\(additionalDirectories)")
        return ResumeSessionResponse(
            modes: ModesInfo(
                currentModeId: "chat",
                availableModes: [ModeInfo(id: "chat", name: "Chat")]
            )
        )
    }

    func handleListSessions(_ request: ListSessionsRequest) async throws -> ListSessionsResponse {
        fatalError("Not used in this test")
    }

    func handleDeleteSession(_ request: DeleteSessionRequest) async throws -> DeleteSessionResponse {
        events.append("delete:\(request.sessionId.value)")
        return DeleteSessionResponse()
    }

    func handleCloseSession(_ request: CloseSessionRequest) async throws -> CloseSessionResponse {
        events.append("close:\(request.sessionId.value)")
        return CloseSessionResponse()
    }

    func handleLogout(_ request: LogoutRequest) async throws -> LogoutResponse {
        events.append("logout")
        return LogoutResponse()
    }

    func handleCancelRequest(_ request: CancelRequestNotification) async throws {
        events.append("cancel-request:\(request.requestId.description)")
    }

    func handleForkSession(_ request: ForkSessionRequest) async throws -> ForkSessionResponse {
        let additionalDirectories = request.additionalDirectories?.joined(separator: ",") ?? ""
        events.append("fork:\(request.sessionId.value):\(request.cwd):\(additionalDirectories)")
        return ForkSessionResponse(sessionId: SessionId("session-forked"))
    }

    func handleListProviders(_ request: ListProvidersRequest) async throws -> ListProvidersResponse {
        events.append("providers-list")
        return ListProvidersResponse(providers: [
            ProviderInfo(providerId: "anthropic", supported: [.anthropic], required: false),
        ])
    }

    func handleSuggestNes(_ request: SuggestNesRequest) async throws -> SuggestNesResponse {
        events.append("nes-suggest:\(request.sessionId.value):\(request.triggerKind.value)")
        return SuggestNesResponse(suggestions: [
            NesSuggestion(kind: "jump", id: "sug-1", uri: request.uri, position: request.position),
        ])
    }

    func handleRejectNes(_ notification: RejectNesNotification) async throws {
        events.append("nes-reject:\(notification.sessionId.value):\(notification.id):\(notification.reason?.value ?? "")")
    }

    func handleDidFocusDocument(_ notification: DidFocusDocumentNotification) async throws {
        events.append("document-focus:\(notification.sessionId.value):\(notification.uri)")
    }

    func handleMcpMessage(_ request: MessageMcpRequest) async throws -> MessageMcpResponse {
        events.append("mcp-message:\(request.connectionId.value):\(request.method)")
        return AnyCodable(["ok": true])
    }

    func handleMcpNotification(_ notification: MessageMcpNotification) async throws {
        events.append("mcp-notification:\(notification.connectionId.value):\(notification.method)")
    }

    func recordedEvents() -> [String] {
        events
    }
}

private actor RecordingClientDelegate: ClientDelegate {
    private var events: [String] = []

    func handleFileReadRequest(_ path: String, sessionId: String, line: Int?, limit: Int?) async throws -> ReadTextFileResponse {
        fatalError("Not used in this test")
    }

    func handleFileWriteRequest(_ path: String, content: String, sessionId: String) async throws -> WriteTextFileResponse {
        fatalError("Not used in this test")
    }

    func handleTerminalCreate(command: String, sessionId: String, args: [String]?, cwd: String?, env: [EnvVariable]?, outputByteLimit: Int?) async throws -> CreateTerminalResponse {
        fatalError("Not used in this test")
    }

    func handleTerminalOutput(terminalId: TerminalId, sessionId: String) async throws -> TerminalOutputResponse {
        fatalError("Not used in this test")
    }

    func handleTerminalWaitForExit(terminalId: TerminalId, sessionId: String) async throws -> WaitForExitResponse {
        fatalError("Not used in this test")
    }

    func handleTerminalKill(terminalId: TerminalId, sessionId: String) async throws -> KillTerminalResponse {
        fatalError("Not used in this test")
    }

    func handleTerminalRelease(terminalId: TerminalId, sessionId: String) async throws -> ReleaseTerminalResponse {
        fatalError("Not used in this test")
    }

    func handlePermissionRequest(request: RequestPermissionRequest) async throws -> RequestPermissionResponse {
        fatalError("Not used in this test")
    }

    func handleMcpConnect(_ request: ConnectMcpRequest) async throws -> ConnectMcpResponse {
        events.append("mcp-connect:\(request.serverId.value)")
        return ConnectMcpResponse(connectionId: "conn-1")
    }

    func handleMcpNotification(_ notification: MessageMcpNotification) async throws {
        events.append("mcp-notification:\(notification.connectionId.value):\(notification.method)")
    }

    func handleCreateElicitation(_ request: CreateElicitationRequest) async throws -> CreateElicitationResponse {
        events.append("elicitation-create:\(request.mode)")
        return CreateElicitationResponse(action: "decline")
    }

    func handleCompleteElicitation(_ notification: CompleteElicitationNotification) async throws {
        events.append("elicitation-complete:\(notification.elicitationId.value)")
    }

    func handleCustomRequest(method: String, params: AnyCodable?) async throws -> AnyCodable {
        events.append("custom-request:\(method)")
        return AnyCodable("custom-response")
    }

    func recordedEvents() -> [String] {
        events
    }
}
