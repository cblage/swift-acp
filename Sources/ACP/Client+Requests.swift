import Foundation
import ACPModel
import YYJSON

extension Client {
    public func sendRequest<T: Encodable>(
        method: String,
        params: T,
        timeout: TimeInterval? = nil
    ) async throws -> JSONRPCResponse {
        let deadline = timeout.map { DispatchTime.now() + $0 }
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
            try await writeMessageWithDebug(request, method: method, deadline: deadline)
            if let deadline, DispatchTime.now() >= deadline {
                throw ClientError.requestTimeout
            }
        } catch {
            expectedRequestIds.remove(requestId)
            earlyResponses.removeValue(forKey: requestId)
            throw error
        }
        if let early = earlyResponses.removeValue(forKey: requestId) {
            expectedRequestIds.remove(requestId)
            return early
        }
        // A teardown during the write ends the request here: its sweep clears
        // the id with every pending request, and a continuation registered
        // after it would wait for an answer and a sweep that never come.
        guard expectedRequestIds.contains(requestId) else {
            throw ClientError.processNotRunning
        }
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                pendingRequests[requestId] = continuation
                if let deadline {
                    // The deadline: a work item that spawns a task ONLY when
                    // it fires. A late answer then finds no continuation and
                    // is dropped; the request is abandoned, never awaited.
                    let expiry = DispatchWorkItem { [weak self] in
                        guard let self else { return }
                        Task { await self.failRequest(id: requestId, error: ClientError.requestTimeout) }
                    }
                    pendingTimeouts[requestId] = expiry
                    DispatchQueue.global(qos: .utility).asyncAfter(deadline: deadline, execute: expiry)
                }
            }
        } onCancel: {
            // A cancelled caller is released at once and the request
            // abandoned the same way a timed-out one is.
            Task { await self.failRequest(id: requestId, error: CancellationError()) }
        }
    }
}
