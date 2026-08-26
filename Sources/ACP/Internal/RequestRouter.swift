//
//  RequestRouter.swift
//  ACP
//
//  Routes incoming ACP requests to appropriate handlers
//

import Foundation
import ACPModel

actor ACPRequestRouter {
    // MARK: - Properties

    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    weak var delegate: ClientDelegate?

    // MARK: - Initialization

    init(encoder: JSONEncoder, decoder: JSONDecoder) {
        self.encoder = encoder
        self.decoder = decoder
    }

    // MARK: - Delegate Management

    func setDelegate(_ delegate: ClientDelegate?) {
        self.delegate = delegate
    }

    // MARK: - Request Routing

    func routeRequest(_ request: JSONRPCRequest) async throws -> AnyCodable {
        switch request.method {
        case "fs/read_text_file":
            return try await handleFileRead(request)
        case "fs/write_text_file":
            return try await handleFileWrite(request)
        case "terminal/create":
            return try await handleTerminalCreateRequest(request)
        case "terminal/output":
            return try await handleTerminalOutputRequest(request)
        case "terminal/wait_for_exit":
            return try await handleTerminalWaitForExit(request)
        case "terminal/kill":
            return try await handleTerminalKill(request)
        case "terminal/release":
            return try await handleTerminalRelease(request)
        case "request_permission", "session/request_permission":
            return try await handlePermissionRequestMethod(request)
        case "mcp/connect":
            return try await handleMcpConnect(request)
        case "mcp/message":
            return try await handleMcpMessage(request)
        case "mcp/disconnect":
            return try await handleMcpDisconnect(request)
        case "elicitation/create":
            return try await handleCreateElicitation(request)
        default:
            guard let delegate else {
                throw ClientError.delegateNotSet
            }
            return try await delegate.handleCustomRequest(method: request.method, params: request.params)
        }
    }

    func routeNotification(_ notification: JSONRPCNotification) async throws {
        guard let delegate else { return }

        switch notification.method {
        case "mcp/message":
            guard let params = notification.params else {
                throw ClientError.invalidResponse
            }
            let data = try encoder.encode(params)
            let notification = try decoder.decode(MessageMcpNotification.self, from: data)
            try await delegate.handleMcpNotification(notification)

        case "elicitation/complete":
            guard let params = notification.params else {
                throw ClientError.invalidResponse
            }
            let data = try encoder.encode(params)
            let notification = try decoder.decode(CompleteElicitationNotification.self, from: data)
            try await delegate.handleCompleteElicitation(notification)

        default:
            return
        }
    }

    // MARK: - Request Handlers

    private func handleFileRead(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(ReadTextFileRequest.self, from: data)

        let response = try await delegate.handleFileReadRequest(
            req.path,
            sessionId: req.sessionId,
            line: req.line,
            limit: req.limit
        )

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleFileWrite(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(WriteTextFileRequest.self, from: data)

        let response = try await delegate.handleFileWriteRequest(req.path, content: req.content, sessionId: req.sessionId)

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleTerminalCreateRequest(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(CreateTerminalRequest.self, from: data)

        let response = try await delegate.handleTerminalCreate(
            command: req.command,
            sessionId: req.sessionId,
            args: req.args,
            cwd: req.cwd,
            env: req.env,
            outputByteLimit: req.outputByteLimit
        )

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleTerminalOutputRequest(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(TerminalOutputRequest.self, from: data)

        let response = try await delegate.handleTerminalOutput(terminalId: req.terminalId, sessionId: req.sessionId)

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleTerminalWaitForExit(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(WaitForExitRequest.self, from: data)

        let response = try await delegate.handleTerminalWaitForExit(terminalId: req.terminalId, sessionId: req.sessionId)

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleTerminalKill(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(KillTerminalRequest.self, from: data)

        let response = try await delegate.handleTerminalKill(terminalId: req.terminalId, sessionId: req.sessionId)

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleTerminalRelease(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(ReleaseTerminalRequest.self, from: data)

        let response = try await delegate.handleTerminalRelease(terminalId: req.terminalId, sessionId: req.sessionId)

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handlePermissionRequestMethod(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(RequestPermissionRequest.self, from: data)

        let response = try await delegate.handlePermissionRequest(request: req)

        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleMcpConnect(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(ConnectMcpRequest.self, from: data)
        let response = try await delegate.handleMcpConnect(req)
        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleMcpMessage(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(MessageMcpRequest.self, from: data)
        return try await delegate.handleMcpMessage(req)
    }

    private func handleMcpDisconnect(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(DisconnectMcpRequest.self, from: data)
        let response = try await delegate.handleMcpDisconnect(req)
        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }

    private func handleCreateElicitation(_ request: JSONRPCRequest) async throws -> AnyCodable {
        guard let delegate = delegate else {
            throw ClientError.delegateNotSet
        }

        guard let params = request.params else {
            throw ClientError.invalidResponse
        }

        let data = try encoder.encode(params)
        let req = try decoder.decode(CreateElicitationRequest.self, from: data)
        let response = try await delegate.handleCreateElicitation(req)
        let responseData = try encoder.encode(response)
        return try decoder.decode(AnyCodable.self, from: responseData)
    }
}
