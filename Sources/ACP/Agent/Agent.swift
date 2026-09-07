//
//  Agent.swift
//  ACP
//
//  Agent runtime for building ACP-compliant agents (server mode)
//

import Foundation
import os.log
import ACPModel
import YYJSON

/// Protocol for handling agent operations
public protocol AgentDelegate: AnyObject, Sendable {
    /// Handle initialization request from client
    func handleInitialize(_ request: InitializeRequest) async throws -> InitializeResponse

    /// Handle new session request
    func handleNewSession(_ request: NewSessionRequest) async throws -> NewSessionResponse

    /// Handle prompt request - the main interaction point
    func handlePrompt(_ request: SessionPromptRequest) async throws -> SessionPromptResponse

    /// Handle session cancellation
    func handleCancel(_ sessionId: SessionId) async throws

    /// Handle session load request
    func handleLoadSession(_ request: LoadSessionRequest) async throws -> LoadSessionResponse

    /// Handle session resume request
    func handleResumeSession(_ request: ResumeSessionRequest) async throws -> ResumeSessionResponse

    /// Handle session listing request
    func handleListSessions(_ request: ListSessionsRequest) async throws -> ListSessionsResponse

    /// Handle session delete request
    func handleDeleteSession(_ request: DeleteSessionRequest) async throws -> DeleteSessionResponse

    /// Handle session close request
    func handleCloseSession(_ request: CloseSessionRequest) async throws -> CloseSessionResponse

    /// Handle logout request
    func handleLogout(_ request: LogoutRequest) async throws -> LogoutResponse

    /// Handle protocol-level request cancellation notification
    func handleCancelRequest(_ request: CancelRequestNotification) async throws

    /// Handle fork session request
    func handleForkSession(_ request: ForkSessionRequest) async throws -> ForkSessionResponse

    /// Handle provider listing request
    func handleListProviders(_ request: ListProvidersRequest) async throws -> ListProvidersResponse

    /// Handle provider configuration request
    func handleSetProvider(_ request: SetProviderRequest) async throws -> SetProviderResponse

    /// Handle provider disable request
    func handleDisableProvider(_ request: DisableProviderRequest) async throws -> DisableProviderResponse

    /// Handle NES session start request
    func handleStartNes(_ request: StartNesRequest) async throws -> StartNesResponse

    /// Handle NES suggestion request
    func handleSuggestNes(_ request: SuggestNesRequest) async throws -> SuggestNesResponse

    /// Handle NES close request
    func handleCloseNes(_ request: CloseNesRequest) async throws -> CloseNesResponse

    /// Handle NES accepted suggestion notification
    func handleAcceptNes(_ notification: AcceptNesNotification) async throws

    /// Handle NES rejected suggestion notification
    func handleRejectNes(_ notification: RejectNesNotification) async throws

    /// Handle document open notification
    func handleDidOpenDocument(_ notification: DidOpenDocumentNotification) async throws

    /// Handle document change notification
    func handleDidChangeDocument(_ notification: DidChangeDocumentNotification) async throws

    /// Handle document close notification
    func handleDidCloseDocument(_ notification: DidCloseDocumentNotification) async throws

    /// Handle document save notification
    func handleDidSaveDocument(_ notification: DidSaveDocumentNotification) async throws

    /// Handle document focus notification
    func handleDidFocusDocument(_ notification: DidFocusDocumentNotification) async throws

    /// Handle MCP-over-ACP request message
    func handleMcpMessage(_ request: MessageMcpRequest) async throws -> MessageMcpResponse

    /// Handle MCP-over-ACP notification message
    func handleMcpNotification(_ notification: MessageMcpNotification) async throws
}

/// Default implementations for optional delegate methods
extension AgentDelegate {
    public func handleCancel(_ sessionId: SessionId) async throws {
        // Default: no-op
    }

    public func handleLoadSession(_ request: LoadSessionRequest) async throws -> LoadSessionResponse {
        throw ClientError.invalidResponse
    }

    public func handleResumeSession(_ request: ResumeSessionRequest) async throws -> ResumeSessionResponse {
        throw ClientError.invalidResponse
    }

    public func handleListSessions(_ request: ListSessionsRequest) async throws -> ListSessionsResponse {
        throw ClientError.invalidResponse
    }

    public func handleDeleteSession(_ request: DeleteSessionRequest) async throws -> DeleteSessionResponse {
        throw ClientError.invalidResponse
    }

    public func handleCloseSession(_ request: CloseSessionRequest) async throws -> CloseSessionResponse {
        throw ClientError.invalidResponse
    }

    public func handleLogout(_ request: LogoutRequest) async throws -> LogoutResponse {
        throw ClientError.invalidResponse
    }

    public func handleCancelRequest(_ request: CancelRequestNotification) async throws {
        // Default: no-op
    }

    public func handleForkSession(_ request: ForkSessionRequest) async throws -> ForkSessionResponse {
        throw ClientError.invalidResponse
    }

    public func handleListProviders(_ request: ListProvidersRequest) async throws -> ListProvidersResponse {
        throw ClientError.invalidResponse
    }

    public func handleSetProvider(_ request: SetProviderRequest) async throws -> SetProviderResponse {
        throw ClientError.invalidResponse
    }

    public func handleDisableProvider(_ request: DisableProviderRequest) async throws -> DisableProviderResponse {
        throw ClientError.invalidResponse
    }

    public func handleStartNes(_ request: StartNesRequest) async throws -> StartNesResponse {
        throw ClientError.invalidResponse
    }

    public func handleSuggestNes(_ request: SuggestNesRequest) async throws -> SuggestNesResponse {
        throw ClientError.invalidResponse
    }

    public func handleCloseNes(_ request: CloseNesRequest) async throws -> CloseNesResponse {
        throw ClientError.invalidResponse
    }

    public func handleAcceptNes(_ notification: AcceptNesNotification) async throws {
        // Default: no-op
    }

    public func handleRejectNes(_ notification: RejectNesNotification) async throws {
        // Default: no-op
    }

    public func handleDidOpenDocument(_ notification: DidOpenDocumentNotification) async throws {
        // Default: no-op
    }

    public func handleDidChangeDocument(_ notification: DidChangeDocumentNotification) async throws {
        // Default: no-op
    }

    public func handleDidCloseDocument(_ notification: DidCloseDocumentNotification) async throws {
        // Default: no-op
    }

    public func handleDidSaveDocument(_ notification: DidSaveDocumentNotification) async throws {
        // Default: no-op
    }

    public func handleDidFocusDocument(_ notification: DidFocusDocumentNotification) async throws {
        // Default: no-op
    }

    public func handleMcpMessage(_ request: MessageMcpRequest) async throws -> MessageMcpResponse {
        throw ClientError.invalidResponse
    }

    public func handleMcpNotification(_ notification: MessageMcpNotification) async throws {
        // Default: no-op
    }
}

/// Incoming request from a client that the agent must handle
public struct AgentRequest: Sendable {
    public let id: RequestId
    public let method: String
    public let params: AnyCodable?

    public init(id: RequestId, method: String, params: AnyCodable?) {
        self.id = id
        self.method = method
        self.params = params
    }
}

/// Agent runtime that receives requests from clients and sends responses/updates
public actor Agent {
    // MARK: - Properties

    private let transport: any Transport
    private let logger: Logger
    private let encoder: YYJSONEncoder
    private let decoder: YYJSONDecoder

    private weak var delegate: AgentDelegate?
    private var pendingRequests: [RequestId: CheckedContinuation<JSONRPCResponse, Error>] = [:]
    private var nextRequestId: Int = 1

    private var requestContinuation: AsyncStream<AgentRequest>.Continuation?
    private let requestStream: AsyncStream<AgentRequest>

    // MARK: - Public API

    /// Stream of incoming requests from the client
    public nonisolated var requests: AsyncStream<AgentRequest> {
        requestStream
    }

    // MARK: - Initialization

    public init(transport: any Transport) {
        self.transport = transport
        self.logger = Logger.forCategory("Agent")
        self.encoder = YYJSONEncoder()
        self.decoder = YYJSONDecoder()

        var continuation: AsyncStream<AgentRequest>.Continuation!
        self.requestStream = AsyncStream { cont in
            continuation = cont
        }
        self.requestContinuation = continuation
    }

    public func setDelegate(_ delegate: AgentDelegate?) {
        self.delegate = delegate
    }

    /// Start processing incoming messages from the transport
    public func start() async {
        for await data in transport.messages {
            await handleMessage(data)
        }
        requestContinuation?.finish()
    }

    /// Send a session update notification to the client
    public func sendUpdate(sessionId: SessionId, update: SessionUpdate) async throws {
        let notification = SessionUpdateNotification(sessionId: sessionId, update: update)
        let paramsData = try encoder.encode(notification)
        let params = try decoder.decode(AnyCodable.self, from: paramsData)

        let message = JSONRPCNotification(method: "session/update", params: params)
        let data = try encoder.encode(message)
        try await transport.send(data)
    }

    /// Send an agent message chunk update
    public func sendMessageChunk(sessionId: SessionId, text: String) async throws {
        let content = ContentBlock.text(TextContent(text: text))
        let update = SessionUpdate.agentMessageChunk(content)
        try await sendUpdate(sessionId: sessionId, update: update)
    }

    /// Send a tool call update
    public func sendToolCall(sessionId: SessionId, toolCall: ToolCallUpdate) async throws {
        let update = SessionUpdate.toolCall(toolCall)
        try await sendUpdate(sessionId: sessionId, update: update)
    }

    /// Send a session metadata update notification
    public func sendSessionInfoUpdate(sessionId: SessionId, info: SessionInfoUpdate) async throws {
        try await sendUpdate(sessionId: sessionId, update: .sessionInfoUpdate(info))
    }

    /// Send a session usage update notification
    public func sendUsageUpdate(sessionId: SessionId, usage: UsageUpdate) async throws {
        try await sendUpdate(sessionId: sessionId, update: .usageUpdate(usage))
    }

    /// Request an MCP-over-ACP connection from the client.
    public func connectMcp(serverId: McpServerAcpId) async throws -> ConnectMcpResponse {
        let request = ConnectMcpRequest(serverId: serverId)
        let response = try await sendRequest(method: "mcp/connect", params: request)
        return try decodeResponse(ConnectMcpResponse.self, from: response)
    }

    /// Send an MCP-over-ACP request message to the client.
    public func sendMcpMessage(
        connectionId: McpConnectionId,
        method: String,
        params: AnyCodable? = nil
    ) async throws -> MessageMcpResponse {
        let request = MessageMcpRequest(connectionId: connectionId, method: method, params: params)
        let response = try await sendRequest(method: "mcp/message", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        return result
    }

    /// Send an MCP-over-ACP notification message to the client.
    public func sendMcpMessageNotification(
        connectionId: McpConnectionId,
        method: String,
        params: AnyCodable? = nil
    ) async throws {
        try await sendNotification(
            method: "mcp/message",
            params: MessageMcpNotification(connectionId: connectionId, method: method, params: params)
        )
    }

    /// Disconnect an MCP-over-ACP connection on the client.
    public func disconnectMcp(connectionId: McpConnectionId) async throws -> DisconnectMcpResponse {
        let request = DisconnectMcpRequest(connectionId: connectionId)
        let response = try await sendRequest(method: "mcp/disconnect", params: request)
        return try decodeEmptyTolerantResponse(DisconnectMcpResponse.self, from: response, emptyValue: DisconnectMcpResponse())
    }

    /// Request structured user input from the client.
    public func createElicitation(_ request: CreateElicitationRequest) async throws -> CreateElicitationResponse {
        let response = try await sendRequest(method: "elicitation/create", params: request)
        return try decodeResponse(CreateElicitationResponse.self, from: response)
    }

    /// Notify the client that a URL-based elicitation completed.
    public func completeElicitation(elicitationId: ElicitationId) async throws {
        try await sendNotification(
            method: "elicitation/complete",
            params: CompleteElicitationNotification(elicitationId: elicitationId)
        )
    }

    /// Close the agent
    public func close() async {
        await transport.close()
        for (_, continuation) in pendingRequests {
            continuation.resume(throwing: ClientError.connectionClosed)
        }
        pendingRequests.removeAll()
        requestContinuation?.finish()
    }

    // MARK: - Private Methods

    private func handleMessage(_ data: Data) async {
        do {
            let message = try decoder.decode(Message.self, from: data)

            switch message {
            case .request(let request):
                await handleRequest(request)
            case .notification(let notification):
                await handleNotification(notification)
            case .response(let response):
                await handleResponse(response)
            }
        } catch {
            logger.error("Failed to decode message: \(error.localizedDescription)")
        }
    }

    private func handleRequest(_ request: JSONRPCRequest) async {
        do {
            let response = try await routeRequest(request)
            try await sendResponse(id: request.id, result: response)
        } catch {
            try? await sendErrorResponse(
                id: request.id,
                code: -32603,
                message: error.localizedDescription
            )
        }
    }

    private func routeRequest(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate else {
            throw ClientError.delegateNotSet
        }

        switch request.method {
        case "initialize":
            let params = try decodeParams(InitializeRequest.self, from: request.params)
            let response = try await delegate.handleInitialize(params)
            return try encodeResult(response)

        case "session/new":
            let params = try decodeParams(NewSessionRequest.self, from: request.params)
            let response = try await delegate.handleNewSession(params)
            return try encodeResult(response)

        case "session/prompt":
            let params = try decodeParams(SessionPromptRequest.self, from: request.params)
            let response = try await delegate.handlePrompt(params)
            return try encodeResult(response)

        case "session/load":
            let params = try decodeParams(LoadSessionRequest.self, from: request.params)
            let response = try await delegate.handleLoadSession(params)
            return try encodeResult(response)

        case "session/resume":
            let params = try decodeParams(ResumeSessionRequest.self, from: request.params)
            let response = try await delegate.handleResumeSession(params)
            return try encodeResult(response)

        case "session/fork":
            let params = try decodeParams(ForkSessionRequest.self, from: request.params)
            let response = try await delegate.handleForkSession(params)
            return try encodeResult(response)

        case "session/list":
            let params = try decodeParams(ListSessionsRequest.self, from: request.params)
            let response = try await delegate.handleListSessions(params)
            return try encodeResult(response)

        case "session/delete":
            let params = try decodeParams(DeleteSessionRequest.self, from: request.params)
            let response = try await delegate.handleDeleteSession(params)
            return try encodeResult(response)

        case "session/close":
            let params = try decodeParams(CloseSessionRequest.self, from: request.params)
            try await delegate.handleCancel(params.sessionId)
            let response = try await delegate.handleCloseSession(params)
            return try encodeResult(response)

        case "logout":
            let params = try decodeParamsIfPresent(LogoutRequest.self, from: request.params)
            let response = try await delegate.handleLogout(params)
            return try encodeResult(response)

        case "providers/list":
            let params = try decodeParamsIfPresent(ListProvidersRequest.self, from: request.params)
            let response = try await delegate.handleListProviders(params)
            return try encodeResult(response)

        case "providers/set":
            let params = try decodeParams(SetProviderRequest.self, from: request.params)
            let response = try await delegate.handleSetProvider(params)
            return try encodeResult(response)

        case "providers/disable":
            let params = try decodeParams(DisableProviderRequest.self, from: request.params)
            let response = try await delegate.handleDisableProvider(params)
            return try encodeResult(response)

        case "nes/start":
            let params = try decodeParamsIfPresent(StartNesRequest.self, from: request.params)
            let response = try await delegate.handleStartNes(params)
            return try encodeResult(response)

        case "nes/suggest":
            let params = try decodeParams(SuggestNesRequest.self, from: request.params)
            let response = try await delegate.handleSuggestNes(params)
            return try encodeResult(response)

        case "nes/close":
            let params = try decodeParams(CloseNesRequest.self, from: request.params)
            let response = try await delegate.handleCloseNes(params)
            return try encodeResult(response)

        case "mcp/message":
            let params = try decodeParams(MessageMcpRequest.self, from: request.params)
            return try await delegate.handleMcpMessage(params)

        default:
            // Emit to request stream for custom handling
            requestContinuation?.yield(AgentRequest(
                id: request.id,
                method: request.method,
                params: request.params
            ))
            throw ClientError.invalidResponse
        }
    }

    private func handleNotification(_ notification: JSONRPCNotification) async {
        switch notification.method {
        case "session/cancel":
            if let params = notification.params,
               let dict = params.value as? [String: Any],
               let sessionIdValue = dict["sessionId"] as? String {
                let sessionId = SessionId(sessionIdValue)
                try? await delegate?.handleCancel(sessionId)
            }
        case "$/cancel_request":
            if let request = try? decodeParams(CancelRequestNotification.self, from: notification.params) {
                try? await delegate?.handleCancelRequest(request)
            }
        case "nes/accept":
            if let notification = try? decodeParams(AcceptNesNotification.self, from: notification.params) {
                try? await delegate?.handleAcceptNes(notification)
            }
        case "nes/reject":
            if let notification = try? decodeParams(RejectNesNotification.self, from: notification.params) {
                try? await delegate?.handleRejectNes(notification)
            }
        case "document/didOpen":
            if let notification = try? decodeParams(DidOpenDocumentNotification.self, from: notification.params) {
                try? await delegate?.handleDidOpenDocument(notification)
            }
        case "document/didChange":
            if let notification = try? decodeParams(DidChangeDocumentNotification.self, from: notification.params) {
                try? await delegate?.handleDidChangeDocument(notification)
            }
        case "document/didClose":
            if let notification = try? decodeParams(DidCloseDocumentNotification.self, from: notification.params) {
                try? await delegate?.handleDidCloseDocument(notification)
            }
        case "document/didSave":
            if let notification = try? decodeParams(DidSaveDocumentNotification.self, from: notification.params) {
                try? await delegate?.handleDidSaveDocument(notification)
            }
        case "document/didFocus":
            if let notification = try? decodeParams(DidFocusDocumentNotification.self, from: notification.params) {
                try? await delegate?.handleDidFocusDocument(notification)
            }
        case "mcp/message":
            if let notification = try? decodeParams(MessageMcpNotification.self, from: notification.params) {
                try? await delegate?.handleMcpNotification(notification)
            }
        default:
            logger.debug("Unhandled notification: \(notification.method)")
        }
    }

    private func handleResponse(_ response: JSONRPCResponse) async {
        guard let continuation = pendingRequests.removeValue(forKey: response.id) else {
            logger.warning("Received response for unknown request id=\(response.id)")
            return
        }
        continuation.resume(returning: response)
    }

    private func sendRequest<T: Encodable>(method: String, params: T) async throws -> JSONRPCResponse {
        let requestId = RequestId.number(nextRequestId)
        nextRequestId += 1

        let paramsData = try encoder.encode(params)
        let paramsValue = try decoder.decode(AnyCodable.self, from: paramsData)

        let request = JSONRPCRequest(
            id: requestId,
            method: method,
            params: paramsValue
        )

        return try await withCheckedThrowingContinuation { continuation in
            pendingRequests[requestId] = continuation

            Task {
                do {
                    let data = try self.encoder.encode(request)
                    try await self.transport.send(data)
                } catch {
                    await self.failRequest(id: requestId, error: error)
                }
            }
        }
    }

    private func sendNotification<T: Encodable>(method: String, params: T) async throws {
        let paramsData = try encoder.encode(params)
        let paramsValue = try decoder.decode(AnyCodable.self, from: paramsData)

        let notification = JSONRPCNotification(
            method: method,
            params: paramsValue
        )
        let data = try encoder.encode(notification)
        try await transport.send(data)
    }

    private func failRequest(id: RequestId, error: Error) async {
        if let continuation = pendingRequests.removeValue(forKey: id) {
            continuation.resume(throwing: error)
        }
    }

    private func decodeResponse<T: Decodable>(_ type: T.Type, from response: JSONRPCResponse) throws -> T {
        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(type, from: data)
    }

    private func decodeEmptyTolerantResponse<T: Decodable>(
        _ type: T.Type,
        from response: JSONRPCResponse,
        emptyValue: @autoclosure () -> T
    ) throws -> T {
        if let error = response.error {
            throw ClientError.agentError(error)
        }

        if response.result == nil || (response.result?.value is NSNull) {
            return emptyValue()
        }

        if let dict = response.result?.value as? [String: Any], dict.isEmpty {
            return emptyValue()
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(type, from: data)
    }

    private func sendResponse(id: RequestId, result: AnyCodable) async throws {
        let response = JSONRPCResponse(id: id, result: result, error: nil)
        let data = try encoder.encode(response)
        try await transport.send(data)
    }

    private func sendErrorResponse(id: RequestId, code: Int, message: String) async throws {
        let error = JSONRPCError(code: code, message: message, data: nil)
        let response = JSONRPCResponse(id: id, result: nil, error: error)
        let data = try encoder.encode(response)
        try await transport.send(data)
    }

    private func decodeParams<T: Decodable>(_ type: T.Type, from params: AnyCodable?) throws -> T {
        guard let params else {
            throw ClientError.invalidResponse
        }
        let data = try encoder.encode(params)
        return try decoder.decode(type, from: data)
    }

    private func decodeParamsIfPresent<T: Decodable>(_ type: T.Type, from params: AnyCodable?) throws -> T {
        guard let params, !(params.value is NSNull) else {
            return try decoder.decode(type, from: Data("{}".utf8))
        }
        let data = try encoder.encode(params)
        return try decoder.decode(type, from: data)
    }

    private func encodeResult<T: Encodable>(_ result: T) throws -> AnyCodable {
        let data = try encoder.encode(result)
        return try decoder.decode(AnyCodable.self, from: data)
    }
}
