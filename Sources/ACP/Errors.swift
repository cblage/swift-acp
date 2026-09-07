//
//  Errors.swift
//  ACP
//
//  Error handling for ACP client
//

import Foundation
import ACPModel
import YYJSON

// ClientError is now defined in ACPModel
// This file contains the ErrorHandler actor which is ACP-specific

actor ErrorHandler {
    // MARK: - Properties

    /// Its own encoder, like every actor here: the yyjson coders are value
    /// types that are not `Sendable`.
    private let encoder = ACPJSONEncoder()

    // MARK: - Initialization

    init() {}

    // MARK: - Error Response Creation

    func createErrorResponse(
        requestId: RequestId,
        code: Int,
        message: String
    ) throws -> JSONRPCResponse {
        let error = JSONRPCError(code: code, message: message, data: nil)
        return JSONRPCResponse(id: requestId, result: nil, error: error)
    }

    // MARK: - Error Handling

    func handleError(_ error: Error) -> String {
        if let clientError = error as? ClientError {
            return clientError.errorDescription ?? error.localizedDescription
        }
        return error.localizedDescription
    }

    func extractAgentError(from response: JSONRPCResponse) -> Error? {
        if let error = response.error {
            return ClientError.agentError(error)
        }
        return nil
    }
}
