//
//  Client.swift
//  ACP
//
//  Actor-based ACP agent subprocess manager
//

import Foundation
import os.log
import ACPModel

// MARK: - Debug Message Types

public enum DebugMessageDirection: Sendable {
    case outgoing
    case incoming
}

public struct DebugMessage: Sendable {
    public let direction: DebugMessageDirection
    public let timestamp: Date
    public let rawData: Data
    public let method: String?

    public var jsonString: String? {
        String(data: rawData, encoding: .utf8)
    }
}

public actor Client {
    // MARK: - Properties

    private let logger = Logger.forCategory("Client")

    private let processManager: ACPProcessManager
    private let requestRouter: ACPRequestRouter
    private let errorHandler: ErrorHandler

    private var pendingRequests: [RequestId: CheckedContinuation<JSONRPCResponse, Error>] = [:]
    /// A deadline per bounded request — a GCD work item, NOT a task. The old
    /// timeout raced the request against a sleeping child in a task group,
    /// which on expiry threw out of the group and then awaited the very
    /// child a silent agent had parked, so it never returned; and every
    /// request cost one task to sleep and another to register and write.
    /// The item fires only when the bound actually lapses; a request that
    /// answers in time cancels it and creates nothing.
    private var pendingTimeouts: [RequestId: DispatchWorkItem] = [:]
    /// Requests written but not yet awaited: a response arriving in the gap
    /// while the write suspends this actor parks in `earlyResponses` for the
    /// continuation that registers right after it.
    private var expectedRequestIds: Set<RequestId> = []
    private var earlyResponses: [RequestId: JSONRPCResponse] = [:]
    private var nextRequestId: Int = 1

    private let notificationContinuation: AsyncStream<JSONRPCNotification>.Continuation
    private let notificationStream: AsyncStream<JSONRPCNotification>
    private var notificationsYielded = 0

    /// The receive path's classifier: `method` names a request or a
    /// notification, `id` tells them apart, and a `null` id reads as absent
    /// — the same rule `Message` applies. An id that decodes as neither
    /// string nor number fails the decode, and the message takes the full
    /// path instead.
    private struct MessageEnvelope: Decodable {
        let method: String?
        let id: RequestId?
    }

    private var debugContinuation: AsyncStream<DebugMessage>.Continuation?
    private var debugStream: AsyncStream<DebugMessage>?

    private let decoder: JSONDecoder
    private let encoder: JSONEncoder

    public weak var delegate: ClientDelegate?

    // MARK: - Initialization

    public init() {
        decoder = JSONDecoder()
        encoder = JSONEncoder()
        encoder.outputFormatting = [.withoutEscapingSlashes]

        var continuation: AsyncStream<JSONRPCNotification>.Continuation!
        notificationStream = AsyncStream { cont in
            continuation = cont
        }
        notificationContinuation = continuation

        processManager = ACPProcessManager(encoder: encoder, decoder: decoder)
        requestRouter = ACPRequestRouter(encoder: encoder, decoder: decoder)
        errorHandler = ErrorHandler(encoder: encoder)

        Task {
            await processManager.setDataReceivedCallback { [weak self] data in
                await self?.handleMessage(data: data)
            }
            await processManager.setTerminationCallback { [weak self] exitCode in
                await self?.handleTermination(exitCode: exitCode)
            }
        }
    }

    // MARK: - Public API

    public var notifications: AsyncStream<JSONRPCNotification> {
        notificationStream
    }

    /// How many notifications have been handed to `notifications` so far.
    /// Messages are handled in the order the agent sent them and each
    /// notification is counted BEFORE it is yielded, so the count read after
    /// a response covers every notification the agent sent ahead of that
    /// response: a consumer that has taken this many has seen all of them,
    /// whatever the stream still buffers. A time-based quiet wait in the
    /// consumer cannot establish that — it only held while the reader was
    /// slow enough never to build a backlog.
    public var notificationsDelivered: Int {
        notificationsYielded
    }

    public var debugMessages: AsyncStream<DebugMessage>? {
        debugStream
    }

    public func enableDebugStream() {
        guard debugStream == nil else { return }
        var continuation: AsyncStream<DebugMessage>.Continuation!
        debugStream = AsyncStream { cont in
            continuation = cont
        }
        debugContinuation = continuation
    }

    public func disableDebugStream() {
        debugContinuation?.finish()
        debugContinuation = nil
        debugStream = nil
    }

    /// The running agent process identifier, when this client launched one.
    public func processIdentifier() async -> Int32? {
        await processManager.processIdentifier()
    }

    /// The running agent process group identifier, when process grouping succeeded.
    public func processGroupIdentifier() async -> Int32? {
        await processManager.processGroupIdentifier()
    }

    /// Complete stderr lines emitted by the running agent.
    public func stderrLines() async -> AsyncStream<String>? {
        await processManager.stderrLines()
    }

    public func setDelegate(_ delegate: ClientDelegate?) {
        self.delegate = delegate
        Task {
            await requestRouter.setDelegate(delegate)
        }
    }

    public func launch(
        agentPath: String,
        arguments: [String] = [],
        workingDirectory: String? = nil,
        environment: [String: String]? = nil
    ) async throws {
        try await processManager.launch(
            agentPath: agentPath,
            arguments: arguments,
            workingDirectory: workingDirectory,
            environment: environment
        )
    }

    public func initialize(
        protocolVersion: Int = 1,
        capabilities: ClientCapabilities,
        clientInfo: ClientInfo? = nil,
        timeout: TimeInterval? = nil
    ) async throws -> InitializeResponse {
        let info = clientInfo ?? ClientInfo(
            name: "ACP",
            title: "ACP Client",
            version: "1.0.0"
        )

        let request = InitializeRequest(
            protocolVersion: protocolVersion,
            clientCapabilities: capabilities,
            clientInfo: info
        )

        let response = try await sendRequest(method: "initialize", params: request, timeout: timeout)

        guard let result = response.result else {
            if let error = response.error {
                throw ClientError.agentError(error)
            }
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(InitializeResponse.self, from: data)
    }

    public func newSession(
        workingDirectory: String,
        additionalDirectories: [String]? = nil,
        mcpServers: [MCPServerConfig] = [],
        timeout: TimeInterval? = nil
    ) async throws -> NewSessionResponse {
        let request = NewSessionRequest(
            cwd: workingDirectory,
            additionalDirectories: additionalDirectories,
            mcpServers: mcpServers
        )

        let response = try await sendRequest(method: "session/new", params: request, timeout: timeout)

        guard let result = response.result else {
            if let error = response.error {
                throw ClientError.agentError(error)
            }
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(NewSessionResponse.self, from: data)
    }

    public func sendPrompt(
        sessionId: SessionId,
        content: [ContentBlock]
    ) async throws -> SessionPromptResponse {
        let request = SessionPromptRequest(
            sessionId: sessionId,
            prompt: content
        )

        let response = try await sendRequest(method: "session/prompt", params: request, timeout: nil)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(SessionPromptResponse.self, from: data)
    }

    public func authenticate(
        authMethodId: String,
        credentials: [String: String]? = nil,
        timeout: TimeInterval? = nil
    ) async throws -> AuthenticateResponse {
        let request = AuthenticateRequest(
            methodId: authMethodId,
            credentials: credentials
        )

        let response = try await sendRequest(method: "authenticate", params: request, timeout: timeout)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        if response.result == nil || (response.result?.value is NSNull) {
            return AuthenticateResponse(success: true, error: nil)
        }

        if let dict = response.result?.value as? [String: Any], dict.isEmpty {
            return AuthenticateResponse(success: true, error: nil)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        do {
            let data = try encoder.encode(result)
            return try decoder.decode(AuthenticateResponse.self, from: data)
        } catch {
            return AuthenticateResponse(success: true, error: nil)
        }
    }

    public func setMode(
        sessionId: SessionId,
        modeId: String
    ) async throws -> SetModeResponse {
        let request = SetModeRequest(
            sessionId: sessionId,
            modeId: modeId
        )

        let response = try await sendRequest(method: "session/set_mode", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        if response.result == nil || (response.result?.value is NSNull) {
            return SetModeResponse()
        }

        if let dict = response.result?.value as? [String: Any], dict.isEmpty {
            return SetModeResponse()
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        do {
            let data = try encoder.encode(result)
            return try decoder.decode(SetModeResponse.self, from: data)
        } catch {
            return SetModeResponse()
        }
    }

    public func setModel(
        sessionId: SessionId,
        modelId: String
    ) async throws -> SetModelResponse {
        let request = SetModelRequest(
            sessionId: sessionId,
            modelId: modelId
        )

        let response = try await sendRequest(method: "session/set_model", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        if response.result == nil || (response.result?.value is NSNull) {
            return SetModelResponse()
        }

        if let dict = response.result?.value as? [String: Any], dict.isEmpty {
            return SetModelResponse()
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        do {
            let data = try encoder.encode(result)
            return try decoder.decode(SetModelResponse.self, from: data)
        } catch {
            return SetModelResponse()
        }
    }

    public func setConfigOption(
        sessionId: SessionId,
        configId: SessionConfigId,
        value: SessionConfigValueId,
        timeout: TimeInterval? = nil
    ) async throws -> SetSessionConfigOptionResponse {
        return try await setConfigOption(
            sessionId: sessionId,
            configId: configId,
            value: .select(value),
            timeout: timeout
        )
    }

    public func setConfigOption(
        sessionId: SessionId,
        configId: SessionConfigId,
        value: Bool
    ) async throws -> SetSessionConfigOptionResponse {
        return try await setConfigOption(
            sessionId: sessionId,
            configId: configId,
            value: .boolean(value)
        )
    }

    public func setConfigOption(
        sessionId: SessionId,
        configId: SessionConfigId,
        value: SessionConfigOptionValue,
        timeout: TimeInterval? = nil
    ) async throws -> SetSessionConfigOptionResponse {
        let request = SetSessionConfigOptionRequest(
            sessionId: sessionId,
            configId: configId,
            value: value
        )

        let response = try await sendRequest(
            method: "session/set_config_option", params: request, timeout: timeout
        )

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(SetSessionConfigOptionResponse.self, from: data)
    }

    public func cancelSession(sessionId: SessionId) async throws {
        try await sendCancelNotification(sessionId: sessionId)
    }

    public func loadSession(
        sessionId: SessionId,
        cwd: String,
        additionalDirectories: [String]? = nil,
        mcpServers: [MCPServerConfig] = [],
        timeout: TimeInterval? = nil
    ) async throws -> LoadSessionResponse {
        let request = LoadSessionRequest(
            sessionId: sessionId,
            cwd: cwd,
            additionalDirectories: additionalDirectories,
            mcpServers: mcpServers
        )

        let response = try await sendRequest(method: "session/load", params: request, timeout: timeout)

        if let error = response.error {
            if isSessionAlreadyActive(error) {
                return LoadSessionResponse(sessionId: sessionId, modes: nil, models: nil, configOptions: nil)
            }
            throw ClientError.agentError(error)
        }

        let extractedSessionId = extractSessionId(from: response.result)

        guard let result = response.result else {
            return LoadSessionResponse(
                sessionId: extractedSessionId,
                modes: nil,
                models: nil,
                configOptions: nil
            )
        }

        let data = try encoder.encode(result)
        if let payload = try? decoder.decode(LoadSessionResponsePayload.self, from: data) {
            return LoadSessionResponse(
                sessionId: payload.sessionId ?? extractedSessionId,
                modes: payload.modes,
                models: payload.models,
                configOptions: payload.configOptions
            )
        }

        if let decoded = try? decoder.decode(LoadSessionResponse.self, from: data) {
            return decoded
        }

        return LoadSessionResponse(
            sessionId: extractedSessionId,
            modes: nil,
            models: nil,
            configOptions: nil
        )
    }

    public func resumeSession(
        sessionId: SessionId,
        cwd: String,
        additionalDirectories: [String]? = nil,
        mcpServers: [MCPServerConfig] = [],
        timeout: TimeInterval? = nil
    ) async throws -> ResumeSessionResponse {
        let request = ResumeSessionRequest(
            sessionId: sessionId,
            cwd: cwd,
            additionalDirectories: additionalDirectories,
            mcpServers: mcpServers
        )

        let response = try await sendRequest(method: "session/resume", params: request, timeout: timeout)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        return try decodeEmptyTolerantResponse(
            ResumeSessionResponse.self,
            from: response,
            emptyValue: ResumeSessionResponse()
        )
    }

    public func forkSession(
        sessionId: SessionId,
        cwd: String,
        additionalDirectories: [String]? = nil,
        mcpServers: [MCPServerConfig] = []
    ) async throws -> ForkSessionResponse {
        let request = ForkSessionRequest(
            sessionId: sessionId,
            cwd: cwd,
            additionalDirectories: additionalDirectories,
            mcpServers: mcpServers
        )

        let response = try await sendRequest(method: "session/fork", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(ForkSessionResponse.self, from: data)
    }

    public func listSessions(
        cwd: String? = nil,
        cursor: String? = nil,
        timeout: TimeInterval? = nil
    ) async throws -> ListSessionsResponse {
        let request = ListSessionsRequest(cwd: cwd, cursor: cursor)
        let response = try await sendRequest(method: "session/list", params: request, timeout: timeout)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(ListSessionsResponse.self, from: data)
    }

    public func closeSession(
        sessionId: SessionId, timeout: TimeInterval? = nil
    ) async throws -> CloseSessionResponse {
        let request = CloseSessionRequest(sessionId: sessionId)
        let response = try await sendRequest(method: "session/close", params: request, timeout: timeout)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        return try decodeEmptyTolerantResponse(
            CloseSessionResponse.self,
            from: response,
            emptyValue: CloseSessionResponse()
        )
    }

    public func deleteSession(sessionId: SessionId) async throws -> DeleteSessionResponse {
        let request = DeleteSessionRequest(sessionId: sessionId)
        let response = try await sendRequest(method: "session/delete", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        return try decodeEmptyTolerantResponse(
            DeleteSessionResponse.self,
            from: response,
            emptyValue: DeleteSessionResponse()
        )
    }

    public func logout() async throws -> LogoutResponse {
        let response = try await sendRequest(method: "logout", params: LogoutRequest())

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        return try decodeEmptyTolerantResponse(
            LogoutResponse.self,
            from: response,
            emptyValue: LogoutResponse()
        )
    }

    public func listProviders() async throws -> ListProvidersResponse {
        let response = try await sendRequest(method: "providers/list", params: ListProvidersRequest())

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(ListProvidersResponse.self, from: data)
    }

    public func setProvider(
        providerId: ProviderId,
        apiType: LlmProtocol,
        baseUrl: String,
        headers: [String: String]? = nil
    ) async throws -> SetProviderResponse {
        let request = SetProviderRequest(
            providerId: providerId,
            apiType: apiType,
            baseUrl: baseUrl,
            headers: headers
        )
        let response = try await sendRequest(method: "providers/set", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        return try decodeEmptyTolerantResponse(
            SetProviderResponse.self,
            from: response,
            emptyValue: SetProviderResponse()
        )
    }

    public func disableProvider(providerId: ProviderId) async throws -> DisableProviderResponse {
        let request = DisableProviderRequest(providerId: providerId)
        let response = try await sendRequest(method: "providers/disable", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        return try decodeEmptyTolerantResponse(
            DisableProviderResponse.self,
            from: response,
            emptyValue: DisableProviderResponse()
        )
    }

    public func startNes(
        workspaceUri: String? = nil,
        workspaceFolders: [WorkspaceFolder]? = nil,
        repository: NesRepository? = nil
    ) async throws -> StartNesResponse {
        let request = StartNesRequest(
            workspaceUri: workspaceUri,
            workspaceFolders: workspaceFolders,
            repository: repository
        )
        let response = try await sendRequest(method: "nes/start", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(StartNesResponse.self, from: data)
    }

    public func suggestNes(
        sessionId: SessionId,
        uri: String,
        version: Int64,
        position: TextPosition,
        selection: ACPModel.TextRange? = nil,
        triggerKind: NesTriggerKind,
        context: NesSuggestContext? = nil
    ) async throws -> SuggestNesResponse {
        let request = SuggestNesRequest(
            sessionId: sessionId,
            uri: uri,
            version: version,
            position: position,
            selection: selection,
            triggerKind: triggerKind,
            context: context
        )
        let response = try await sendRequest(method: "nes/suggest", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        guard let result = response.result else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(result)
        return try decoder.decode(SuggestNesResponse.self, from: data)
    }

    public func acceptNesSuggestion(sessionId: SessionId, id: String) async throws {
        try await sendNotification(
            method: "nes/accept",
            params: AcceptNesNotification(sessionId: sessionId, id: id)
        )
    }

    public func rejectNesSuggestion(sessionId: SessionId, id: String, reason: NesRejectReason? = nil) async throws {
        try await sendNotification(
            method: "nes/reject",
            params: RejectNesNotification(sessionId: sessionId, id: id, reason: reason)
        )
    }

    public func closeNes(sessionId: SessionId) async throws -> CloseNesResponse {
        let request = CloseNesRequest(sessionId: sessionId)
        let response = try await sendRequest(method: "nes/close", params: request)

        if let error = response.error {
            throw ClientError.agentError(error)
        }

        return try decodeEmptyTolerantResponse(
            CloseNesResponse.self,
            from: response,
            emptyValue: CloseNesResponse()
        )
    }

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

    public func didOpenDocument(
        sessionId: SessionId,
        uri: String,
        languageId: String,
        version: Int64,
        text: String
    ) async throws {
        try await sendNotification(
            method: "document/didOpen",
            params: DidOpenDocumentNotification(
                sessionId: sessionId,
                uri: uri,
                languageId: languageId,
                version: version,
                text: text
            )
        )
    }

    public func didChangeDocument(
        sessionId: SessionId,
        uri: String,
        version: Int64,
        contentChanges: [TextDocumentContentChangeEvent]
    ) async throws {
        try await sendNotification(
            method: "document/didChange",
            params: DidChangeDocumentNotification(
                sessionId: sessionId,
                uri: uri,
                version: version,
                contentChanges: contentChanges
            )
        )
    }

    public func didCloseDocument(sessionId: SessionId, uri: String) async throws {
        try await sendNotification(
            method: "document/didClose",
            params: DidCloseDocumentNotification(sessionId: sessionId, uri: uri)
        )
    }

    public func didSaveDocument(sessionId: SessionId, uri: String) async throws {
        try await sendNotification(
            method: "document/didSave",
            params: DidSaveDocumentNotification(sessionId: sessionId, uri: uri)
        )
    }

    public func didFocusDocument(
        sessionId: SessionId,
        uri: String,
        version: Int64,
        position: TextPosition,
        visibleRange: ACPModel.TextRange
    ) async throws {
        try await sendNotification(
            method: "document/didFocus",
            params: DidFocusDocumentNotification(
                sessionId: sessionId,
                uri: uri,
                version: version,
                position: position,
                visibleRange: visibleRange
            )
        )
    }

    private struct LoadSessionResponsePayload: Decodable {
        let sessionId: SessionId?
        let modes: ModesInfo?
        let models: ModelsInfo?
        let configOptions: [SessionConfigOption]?
    }

    private func extractSessionId(from result: AnyCodable?) -> SessionId? {
        guard let value = result?.value else { return nil }

        if let dict = value as? [String: Any] {
            if let id = dict["sessionId"] as? String ?? dict["session_id"] as? String {
                return SessionId(id)
            }
        }

        if let dict = value as? [String: AnyCodable] {
            if let id = dict["sessionId"]?.value as? String ?? dict["session_id"]?.value as? String {
                return SessionId(id)
            }
        }

        return nil
    }

    private func decodeEmptyTolerantResponse<T: Decodable>(
        _ type: T.Type,
        from response: JSONRPCResponse,
        emptyValue: @autoclosure () -> T
    ) throws -> T {
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

    private func isSessionAlreadyActive(_ error: JSONRPCError) -> Bool {
        let message = error.message.lowercased()
        if message.contains("already active") || message.contains("already started") || message.contains("already exists") {
            return true
        }

        if let dataString = error.data?.value as? String {
            let lower = dataString.lowercased()
            if lower.contains("already active") || lower.contains("already started") || lower.contains("already exists") {
                return true
            }
        }

        if let data = error.data?.value as? [String: Any],
           let details = data["details"] as? String {
            let lower = details.lowercased()
            if lower.contains("already active") || lower.contains("already started") || lower.contains("already exists") {
                return true
            }
        }

        return false
    }

    public func sendRequest<T: Encodable>(
        method: String,
        params: T,
        timeout: TimeInterval? = nil
    ) async throws -> JSONRPCResponse {
        guard await processManager.isRunning() else {
            throw ClientError.processNotRunning
        }

        let requestId = RequestId.number(nextRequestId)
        nextRequestId += 1

        let paramsData = try encoder.encode(params)
        let paramsValue = try decoder.decode(AnyCodable.self, from: paramsData)

        let request = JSONRPCRequest(
            id: requestId,
            method: method,
            params: paramsValue
        )
        // TASK-FREE on the ordinary path: the id is expected BEFORE the write,
        // so a response landing while the write suspends this actor parks in
        // `earlyResponses`; the continuation registers synchronously after
        // the write, on this actor, leaving no gap for a response to miss.
        expectedRequestIds.insert(requestId)
        do {
            try await writeMessageWithDebug(request, method: method)
        } catch {
            expectedRequestIds.remove(requestId)
            earlyResponses.removeValue(forKey: requestId)
            throw error
        }
        if let early = earlyResponses.removeValue(forKey: requestId) {
            expectedRequestIds.remove(requestId)
            return early
        }
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                pendingRequests[requestId] = continuation
                if let timeout {
                    // The deadline: a work item that spawns a task ONLY when
                    // it fires. A late answer then finds no continuation and
                    // is dropped; the request is abandoned, never awaited.
                    let deadline = DispatchWorkItem { [weak self] in
                        guard let self else { return }
                        Task { await self.failRequest(id: requestId, error: ClientError.requestTimeout) }
                    }
                    pendingTimeouts[requestId] = deadline
                    DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeout, execute: deadline)
                }
            }
        } onCancel: {
            // A cancelled caller is released at once and the request
            // abandoned the same way a timed-out one is.
            Task { await self.failRequest(id: requestId, error: CancellationError()) }
        }
    }

    public func sendCancelNotification(sessionId: SessionId) async throws {
        guard await processManager.isRunning() else {
            throw ClientError.processNotRunning
        }

        struct CancelParams: Encodable {
            let sessionId: SessionId
        }

        let params = CancelParams(sessionId: sessionId)
        let paramsData = try encoder.encode(params)
        let paramsValue = try decoder.decode(AnyCodable.self, from: paramsData)

        let notification = JSONRPCNotification(
            method: "session/cancel",
            params: paramsValue
        )

        try await writeMessageWithDebug(notification, method: "session/cancel")
    }

    public func sendCancelRequest(requestId: RequestId) async throws {
        guard await processManager.isRunning() else {
            throw ClientError.processNotRunning
        }

        let params = CancelRequestNotification(requestId: requestId)
        let paramsData = try encoder.encode(params)
        let paramsValue = try decoder.decode(AnyCodable.self, from: paramsData)

        let notification = JSONRPCNotification(
            method: "$/cancel_request",
            params: paramsValue
        )

        try await writeMessageWithDebug(notification, method: "$/cancel_request")
    }

    private func sendNotification<T: Encodable>(method: String, params: T) async throws {
        guard await processManager.isRunning() else {
            throw ClientError.processNotRunning
        }

        let paramsData = try encoder.encode(params)
        let paramsValue = try decoder.decode(AnyCodable.self, from: paramsData)

        let notification = JSONRPCNotification(
            method: method,
            params: paramsValue
        )

        try await writeMessageWithDebug(notification, method: method)
    }

    public func terminate() async {
        await processManager.terminate()

        failAllRequests(error: ClientError.processNotRunning)

        notificationContinuation.finish()
        debugContinuation?.finish()
        debugContinuation = nil
        debugStream = nil
    }

    // MARK: - Private Methods

    private func handleMessage(data: Data) async {
        // A byte test, not a String decode plus a whitespace trim: both were
        // full passes over the message, paid before the decoder even ran.
        guard data.contains(where: { $0 != 0x20 && $0 != 0x09 && $0 != 0x0D && $0 != 0x0A }) else {
            return
        }

        if let continuation = debugContinuation {
            let method = extractMethod(from: data)
            continuation.yield(DebugMessage(
                direction: .incoming,
                timestamp: Date(),
                rawData: data,
                method: method
            ))
        }

        do {
            // A NOTIFICATION SKIPS THE TREE: a light envelope classifies the
            // message, and a notification is built on its own bytes with
            // `params` decoding on demand — the full `Message` decode built
            // an `AnyCodable` tree for every notification that a consumer
            // decoding its typed payload from the same bytes never read
            // (2026-09-03 profile: the readers ran flat out through a
            // replay). Requests, responses, and anything the envelope cannot
            // classify — a malformed id — take the full decode as before,
            // whose own rule still reads `"id": null` as a notification.
            if let envelope = try? decoder.decode(MessageEnvelope.self, from: data),
               let method = envelope.method, envelope.id == nil {
                let notification = JSONRPCNotification(method: method, rawData: data)
                notificationsYielded += 1
                notificationContinuation.yield(notification)
                await handleIncomingNotification(notification)
                return
            }

            let message = try decoder.decode(Message.self, from: data)

            switch message {
            case .response(let response):
                await handleResponse(response)

            case .notification(var notification):
                // The bytes ride along, so a consumer decodes its typed
                // payload once from them instead of round-tripping `params`.
                notification.rawData = data
                notificationsYielded += 1
                notificationContinuation.yield(notification)
                await handleIncomingNotification(notification)

            case .request(let request):
                Task { [weak self] in
                    await self?.handleIncomingRequest(request)
                }
            }
        } catch {
            if let text = String(data: data, encoding: .utf8) {
                logger.warning("Failed to parse message: \(error.localizedDescription)\nData: \(text.prefix(500))")
            } else {
                logger.warning("Failed to parse message: \(error.localizedDescription)")
            }
        }
    }

    private func handleResponse(_ response: JSONRPCResponse) async {
        if let continuation = pendingRequests.removeValue(forKey: response.id) {
            expectedRequestIds.remove(response.id)
            pendingTimeouts.removeValue(forKey: response.id)?.cancel()
            continuation.resume(returning: response)
            return
        }
        if expectedRequestIds.contains(response.id) {
            // Answered before the caller reached its continuation — the
            // write suspended this actor and the reply won the race. Held
            // for the registration that follows the write.
            earlyResponses[response.id] = response
            return
        }
        let stillPending = pendingRequests.keys.map { String(describing: $0) }
        logger.warning("Received response for unknown request id=\(response.id), no pending request found. Pending: \(stillPending)")
    }

    private func handleIncomingRequest(_ request: JSONRPCRequest) async {
        do {
            let response = try await requestRouter.routeRequest(request)
            try await sendSuccessResponse(requestId: request.id, result: response)
        } catch {
            logger.error("Error handling request \(request.method): \(error.localizedDescription)")

            if let clientError = error as? ClientError, case .invalidResponse = clientError {
                try? await sendErrorResponse(
                    requestId: request.id,
                    code: -32601,
                    message: "Method not found: \(request.method)"
                )
            } else {
                try? await sendErrorResponse(
                    requestId: request.id,
                    code: -32603,
                    message: "Internal error: \(error.localizedDescription)"
                )
            }
        }
    }

    private func handleIncomingNotification(_ notification: JSONRPCNotification) async {
        do {
            try await requestRouter.routeNotification(notification)
        } catch {
            logger.warning("Error handling notification \(notification.method): \(error.localizedDescription)")
        }
    }

    private func sendSuccessResponse(requestId: RequestId, result: AnyCodable) async throws {
        let response = JSONRPCResponse(id: requestId, result: result, error: nil)
        try await writeMessageWithDebug(response, method: nil)
    }

    private func sendErrorResponse(requestId: RequestId, code: Int, message: String) async throws {
        let errorResponse = try await errorHandler.createErrorResponse(
            requestId: requestId,
            code: code,
            message: message
        )
        try await writeMessageWithDebug(errorResponse, method: nil)
    }

    private func handleTermination(exitCode: Int32) async {
        logger.info("Agent process terminated with code: \(exitCode)")

        failAllRequests(error: ClientError.processFailed(exitCode))

        notificationContinuation.finish()
    }

    private func failRequest(id: RequestId, error: Error) async {
        expectedRequestIds.remove(id)
        earlyResponses.removeValue(forKey: id)
        pendingTimeouts.removeValue(forKey: id)?.cancel()
        if let continuation = pendingRequests.removeValue(forKey: id) {
            continuation.resume(throwing: error)
        }
    }

    /// Every pending request fails with `error`, every deadline is cancelled,
    /// and the early-response bookkeeping clears — teardown's one sweep.
    private func failAllRequests(error: Error) {
        for (_, continuation) in pendingRequests {
            continuation.resume(throwing: error)
        }
        pendingRequests.removeAll()
        for (_, deadline) in pendingTimeouts {
            deadline.cancel()
        }
        pendingTimeouts.removeAll()
        expectedRequestIds.removeAll()
        earlyResponses.removeAll()
    }

    private func extractMethod(from data: Data) -> String? {
        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return nil
        }
        return json["method"] as? String
    }

    private func writeMessageWithDebug<T: Encodable>(_ message: T, method: String? = nil) async throws {
        if let continuation = debugContinuation {
            if let data = try? encoder.encode(message) {
                continuation.yield(DebugMessage(
                    direction: .outgoing,
                    timestamp: Date(),
                    rawData: data,
                    method: method
                ))
            }
        }
        try await processManager.writeMessage(message)
    }
}

// MARK: - Typealiases for backward compatibility

@available(*, deprecated, renamed: "Client")
public typealias ACPClient = Client
