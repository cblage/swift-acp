//
//  ACPModelTests.swift
//  ACPModelTests
//
//  Tests for ACPModel types
//

import XCTest
@testable import ACPModel

final class ACPModelTests: XCTestCase {

    // MARK: - Message Tests

    func testJSONRPCRequestEncoding() throws {
        let request = JSONRPCRequest(
            id: .number(1),
            method: "test/method",
            params: AnyCodable(["key": "value"])
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(request)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["jsonrpc"] as? String, "2.0")
        XCTAssertEqual(json["id"] as? Int, 1)
        XCTAssertEqual(json["method"] as? String, "test/method")
    }

    func testJSONRPCResponseDecoding() throws {
        let json = """
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"status": "ok"}
        }
        """

        let decoder = JSONDecoder()
        let response = try decoder.decode(JSONRPCResponse.self, from: json.data(using: .utf8)!)

        XCTAssertEqual(response.id, .number(1))
        XCTAssertNotNil(response.result)
        XCTAssertNil(response.error)
    }

    func testMessageDecoding() throws {
        let requestJson = """
        {"jsonrpc": "2.0", "id": 1, "method": "test", "params": {}}
        """
        let notificationJson = """
        {"jsonrpc": "2.0", "method": "notify", "params": {}}
        """
        let responseJson = """
        {"jsonrpc": "2.0", "id": 1, "result": null}
        """

        let decoder = JSONDecoder()

        let request = try decoder.decode(Message.self, from: requestJson.data(using: .utf8)!)
        if case .request(let r) = request {
            XCTAssertEqual(r.method, "test")
        } else {
            XCTFail("Expected request")
        }

        let notification = try decoder.decode(Message.self, from: notificationJson.data(using: .utf8)!)
        if case .notification(let n) = notification {
            XCTAssertEqual(n.method, "notify")
        } else {
            XCTFail("Expected notification")
        }

        let response = try decoder.decode(Message.self, from: responseJson.data(using: .utf8)!)
        if case .response(let r) = response {
            XCTAssertEqual(r.id, .number(1))
        } else {
            XCTFail("Expected response")
        }
    }

    func testMessageWithNullIdDecodesAsNotification() throws {
        let notificationWithNullId = """
        {"jsonrpc":"2.0","id":null,"method":"session/update","params":{"sessionId":"s1"}}
        """

        let message = try JSONDecoder().decode(Message.self, from: notificationWithNullId.data(using: .utf8)!)
        if case .notification(let notification) = message {
            XCTAssertEqual(notification.method, "session/update")
        } else {
            XCTFail("Expected notification for null id")
        }
    }

    func testMessageWithInvalidIdTypeDecodesAsNotification() throws {
        let notificationWithInvalidId = """
        {"jsonrpc":"2.0","id":{"bad":true},"method":"session/update","params":{"sessionId":"s1"}}
        """

        let message = try JSONDecoder().decode(Message.self, from: notificationWithInvalidId.data(using: .utf8)!)
        if case .notification(let notification) = message {
            XCTAssertEqual(notification.method, "session/update")
        } else {
            XCTFail("Expected notification for invalid id type")
        }
    }

    // MARK: - Session Tests

    func testSessionIdEncoding() throws {
        let sessionId = SessionId("test-session-123")
        let encoder = JSONEncoder()
        let data = try encoder.encode(sessionId)
        let string = String(data: data, encoding: .utf8)

        XCTAssertEqual(string, "\"test-session-123\"")
    }

    func testClientInfoEncoding() throws {
        let info = ClientInfo(name: "TestClient", title: "Test", version: "1.0.0")
        let encoder = JSONEncoder()
        let data = try encoder.encode(info)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["name"] as? String, "TestClient")
        XCTAssertEqual(json["title"] as? String, "Test")
        XCTAssertEqual(json["version"] as? String, "1.0.0")
    }

    func testInitializeProtocolVersionDecodingIsLenient() throws {
        let requestJson = """
        {
            "protocolVersion": "2025-03-26",
            "clientCapabilities": {
                "fs": {"readTextFile": true, "writeTextFile": true},
                "terminal": true
            }
        }
        """
        let responseJson = """
        {
            "protocolVersion": "1",
            "agentCapabilities": {}
        }
        """

        let decoder = JSONDecoder()
        let request = try decoder.decode(InitializeRequest.self, from: requestJson.data(using: .utf8)!)
        let response = try decoder.decode(InitializeResponse.self, from: responseJson.data(using: .utf8)!)

        XCTAssertEqual(request.protocolVersion, 1)
        XCTAssertEqual(response.protocolVersion, 1)
    }

    func testSessionInfoAdditionalDirectoriesDecoding() throws {
        let json = """
        {
            "sessionId": "session-123",
            "cwd": "/tmp/project",
            "additionalDirectories": ["/tmp/shared"],
            "title": "Review ACP"
        }
        """

        let info = try JSONDecoder().decode(SessionInfo.self, from: json.data(using: .utf8)!)

        XCTAssertEqual(info.sessionId.value, "session-123")
        XCTAssertEqual(info.cwd, "/tmp/project")
        XCTAssertEqual(info.additionalDirectories, ["/tmp/shared"])
        XCTAssertEqual(info.title, "Review ACP")
    }

    func testCloseSessionRequestEncoding() throws {
        let request = CloseSessionRequest(sessionId: SessionId("session-123"))
        let encoder = JSONEncoder()
        let data = try encoder.encode(request)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["sessionId"] as? String, "session-123")
    }

    func testNewSessionRequestEncodingIncludesAdditionalDirectories() throws {
        let request = NewSessionRequest(
            cwd: "/tmp/project",
            additionalDirectories: ["/tmp/shared"],
            mcpServers: []
        )
        let encoder = JSONEncoder()
        let data = try encoder.encode(request)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["cwd"] as? String, "/tmp/project")
        XCTAssertEqual(json["additionalDirectories"] as? [String], ["/tmp/shared"])
        XCTAssertEqual((json["mcpServers"] as? [Any])?.count, 0)
    }

    func testCloseSessionResponseEncoding() throws {
        let response = CloseSessionResponse()
        let encoder = JSONEncoder()
        let data = try encoder.encode(response)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertTrue(json.isEmpty)
    }

    func testLoadSessionRequestEncoding() throws {
        let request = LoadSessionRequest(
            sessionId: SessionId("session-123"),
            cwd: "/tmp/project",
            additionalDirectories: ["/tmp/shared"],
            mcpServers: []
        )
        let encoder = JSONEncoder()
        let data = try encoder.encode(request)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["sessionId"] as? String, "session-123")
        XCTAssertEqual(json["cwd"] as? String, "/tmp/project")
        XCTAssertEqual(json["additionalDirectories"] as? [String], ["/tmp/shared"])
        XCTAssertEqual((json["mcpServers"] as? [Any])?.count, 0)
    }

    func testResumeSessionRequestDecodingDefaultsMissingMCPServers() throws {
        let json = """
        {
            "sessionId": "session-123",
            "cwd": "/tmp/project",
            "additionalDirectories": ["/tmp/shared"]
        }
        """

        let request = try JSONDecoder().decode(ResumeSessionRequest.self, from: json.data(using: .utf8)!)

        XCTAssertEqual(request.sessionId.value, "session-123")
        XCTAssertEqual(request.cwd, "/tmp/project")
        XCTAssertEqual(request.additionalDirectories, ["/tmp/shared"])
        XCTAssertTrue(request.mcpServers.isEmpty)
    }

    func testResumeSessionResponseEncodingOmitsEmptyStateByDefault() throws {
        let response = ResumeSessionResponse()
        let encoder = JSONEncoder()
        let data = try encoder.encode(response)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertTrue(json.isEmpty)
    }

    func testDeleteSessionRequestAndResponseEncoding() throws {
        let request = DeleteSessionRequest(sessionId: SessionId("session-123"))
        let response = DeleteSessionResponse()
        let encoder = JSONEncoder()

        let requestJson = try JSONSerialization.jsonObject(with: encoder.encode(request)) as! [String: Any]
        let responseJson = try JSONSerialization.jsonObject(with: encoder.encode(response)) as! [String: Any]

        XCTAssertEqual(requestJson["sessionId"] as? String, "session-123")
        XCTAssertTrue(responseJson.isEmpty)
    }

    func testLogoutRequestAndResponseEncoding() throws {
        let encoder = JSONEncoder()
        let requestJson = try JSONSerialization.jsonObject(with: encoder.encode(LogoutRequest())) as! [String: Any]
        let responseJson = try JSONSerialization.jsonObject(with: encoder.encode(LogoutResponse())) as! [String: Any]

        XCTAssertTrue(requestJson.isEmpty)
        XCTAssertTrue(responseJson.isEmpty)
    }

    func testAgentCapabilitiesDecodeStableAuthAndSessionFeatures() throws {
        let json = """
        {
            "auth": {"logout": {}},
            "sessionCapabilities": {
                "additionalDirectories": {},
                "delete": {},
                "resume": {}
            }
        }
        """

        let capabilities = try JSONDecoder().decode(AgentCapabilities.self, from: json.data(using: .utf8)!)

        XCTAssertNotNil(capabilities.auth?.logout)
        XCTAssertNotNil(capabilities.sessionCapabilities?.additionalDirectories)
        XCTAssertNotNil(capabilities.sessionCapabilities?.delete)
        XCTAssertNotNil(capabilities.sessionCapabilities?.resume)
    }

    func testLoadSessionResponseEncodingOmitsSessionIdByDefault() throws {
        let response = LoadSessionResponse()
        let encoder = JSONEncoder()
        let data = try encoder.encode(response)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertNil(json["sessionId"])
    }

    func testKillTerminalResponseEncodingOmitsSuccessByDefault() throws {
        let response = KillTerminalResponse()
        let encoder = JSONEncoder()
        let data = try encoder.encode(response)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertNil(json["success"])
    }

    func testReleaseTerminalResponseEncodingOmitsSuccessByDefault() throws {
        let response = ReleaseTerminalResponse()
        let encoder = JSONEncoder()
        let data = try encoder.encode(response)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertNil(json["success"])
    }

    func testSetModeResponseEncodingOmitsSuccessByDefault() throws {
        let response = SetModeResponse()
        let encoder = JSONEncoder()
        let data = try encoder.encode(response)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertNil(json["success"])
    }

    func testSetModelResponseEncodingOmitsSuccessByDefault() throws {
        let response = SetModelResponse()
        let encoder = JSONEncoder()
        let data = try encoder.encode(response)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertNil(json["success"])
    }

    func testRequestPermissionRequestEncodingUsesToolCallUpdateShape() throws {
        let request = RequestPermissionRequest(
            options: [PermissionOption(kind: "allow_once", name: "Allow once", optionId: "allow-once")],
            sessionId: SessionId("session-123"),
            toolCall: ToolCallUpdate(
                toolCallId: "tool-123",
                status: .pending,
                title: "Read /tmp/file.txt"
            )
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(request)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        let toolCall = try XCTUnwrap(json["toolCall"] as? [String: Any])

        XCTAssertNil(json["message"])
        XCTAssertEqual(json["sessionId"] as? String, "session-123")
        XCTAssertEqual((json["options"] as? [[String: Any]])?.count, 1)
        XCTAssertEqual(toolCall["toolCallId"] as? String, "tool-123")
        XCTAssertEqual(toolCall["status"] as? String, "pending")
        XCTAssertEqual(toolCall["title"] as? String, "Read /tmp/file.txt")
    }

    func testRequestPermissionRequestDecodingAllowsMinimalToolCallUpdateShape() throws {
        let json = """
        {
            "sessionId": "session-123",
            "options": [
                {"kind": "allow_once", "name": "Allow once", "optionId": "allow-once"}
            ],
            "toolCall": {
                "toolCallId": "tool-123",
                "rawInput": {"path": "/tmp/file.txt"}
            }
        }
        """

        let decoder = JSONDecoder()
        let request = try decoder.decode(RequestPermissionRequest.self, from: json.data(using: .utf8)!)

        XCTAssertEqual(request.sessionId.value, "session-123")
        XCTAssertEqual(request.toolCall.toolCallId, "tool-123")
        XCTAssertNil(request.toolCall.status)
        let rawInput = try XCTUnwrap(request.toolCall.rawInput?.value as? [String: Any])
        XCTAssertEqual(rawInput["path"] as? String, "/tmp/file.txt")
    }

    // MARK: - Content Tests

    func testTextContentEncoding() throws {
        let content = TextContent(text: "Hello, world!")
        let encoder = JSONEncoder()
        let data = try encoder.encode(content)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["type"] as? String, "text")
        XCTAssertEqual(json["text"] as? String, "Hello, world!")
    }

    func testContentBlockDecoding() throws {
        let textJson = """
        {"type": "text", "text": "Hello"}
        """

        let decoder = JSONDecoder()
        let block = try decoder.decode(ContentBlock.self, from: textJson.data(using: .utf8)!)

        if case .text(let content) = block {
            XCTAssertEqual(content.text, "Hello")
        } else {
            XCTFail("Expected text content")
        }
    }

    // MARK: - Tool Tests

    func testToolKindRawValues() {
        XCTAssertEqual(ToolKind.read.rawValue, "read")
        XCTAssertEqual(ToolKind.edit.rawValue, "edit")
        XCTAssertEqual(ToolKind.execute.rawValue, "execute")
        XCTAssertEqual(ToolKind.switchMode.rawValue, "switch_mode")
    }

    func testToolStatusRawValues() {
        XCTAssertEqual(ToolStatus.pending.rawValue, "pending")
        XCTAssertEqual(ToolStatus.inProgress.rawValue, "in_progress")
        XCTAssertEqual(ToolStatus.completed.rawValue, "completed")
        XCTAssertEqual(ToolStatus.failed.rawValue, "failed")
    }

    // MARK: - Capabilities Tests

    func testClientCapabilitiesEncoding() throws {
        let capabilities = ClientCapabilities(
            fs: FileSystemCapabilities(readTextFile: true, writeTextFile: true),
            terminal: true,
            meta: nil
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(capabilities)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["terminal"] as? Bool, true)
        let fs = json["fs"] as! [String: Any]
        XCTAssertEqual(fs["readTextFile"] as? Bool, true)
        XCTAssertEqual(fs["writeTextFile"] as? Bool, true)
    }

    func testDraftCapabilitiesEncoding() throws {
        let capabilities = ClientCapabilities(
            fs: FileSystemCapabilities(readTextFile: true, writeTextFile: false),
            terminal: false,
            session: ClientSessionCapabilities(
                configOptions: SessionConfigOptionsCapabilities(boolean: BooleanConfigOptionCapabilities())
            ),
            plan: PlanCapabilities(),
            auth: AuthCapabilities(terminal: true),
            elicitation: ElicitationCapabilities(form: ElicitationFormCapabilities(), url: ElicitationUrlCapabilities()),
            nes: ClientNesCapabilities(jump: NesJumpCapabilities(), rename: NesRenameCapabilities()),
            positionEncodings: [.utf8, .utf16]
        )

        let json = try JSONSerialization.jsonObject(with: JSONEncoder().encode(capabilities)) as! [String: Any]

        XCTAssertNotNil(json["session"])
        XCTAssertNotNil(json["plan"])
        XCTAssertEqual((json["auth"] as? [String: Any])?["terminal"] as? Bool, true)
        XCTAssertNotNil(json["elicitation"])
        XCTAssertNotNil(json["nes"])
        XCTAssertEqual(json["positionEncodings"] as? [String], ["utf-8", "utf-16"])
    }

    func testAgentCapabilitiesDecodeDraftFeatures() throws {
        let json = """
        {
            "mcpCapabilities": {"http": true, "acp": true},
            "providers": {},
            "nes": {
                "events": {
                    "document": {
                        "didOpen": {},
                        "didChange": {"syncKind": "incremental"}
                    }
                },
                "context": {
                    "recentFiles": {"maxCount": 5}
                }
            },
            "positionEncoding": "utf-8"
        }
        """

        let capabilities = try JSONDecoder().decode(AgentCapabilities.self, from: json.data(using: .utf8)!)

        XCTAssertEqual(capabilities.mcpCapabilities?.acp, true)
        XCTAssertNotNil(capabilities.providers)
        XCTAssertNotNil(capabilities.nes?.events?.document?.didOpen)
        XCTAssertEqual(capabilities.nes?.events?.document?.didChange?.syncKind, .incremental)
        XCTAssertEqual(capabilities.nes?.context?.recentFiles?.maxCount, 5)
        XCTAssertEqual(capabilities.positionEncoding?.value, "utf-8")
    }

    func testDraftMCPServerAcpEncodingAndLegacyDecode() throws {
        let config = MCPServerConfig.acp(MCPAcpServerConfig(name: "Client MCP", serverId: "server-1"))
        let json = try JSONSerialization.jsonObject(with: JSONEncoder().encode(config)) as! [String: Any]

        XCTAssertEqual(json["type"] as? String, "acp")
        XCTAssertEqual(json["name"] as? String, "Client MCP")
        XCTAssertEqual(json["serverId"] as? String, "server-1")

        let legacy = """
        {"type":"acp","name":"Client MCP","id":"legacy-server"}
        """
        let decoded = try JSONDecoder().decode(MCPServerConfig.self, from: legacy.data(using: .utf8)!)

        if case .acp(let acp) = decoded {
            XCTAssertEqual(acp.serverId.value, "legacy-server")
        } else {
            XCTFail("Expected ACP MCP server config")
        }
    }

    func testPlanUpdateAndRemovedSessionUpdates() throws {
        let itemsJson = """
        {
            "sessionUpdate": "plan_update",
            "plan": {
                "type": "items",
                "id": "p1",
                "entries": [
                    {"content": "Ship draft support", "priority": "high", "status": "in_progress"}
                ]
            }
        }
        """
        let removedJson = """
        {
            "sessionUpdate": "plan_removed",
            "id": "p1"
        }
        """

        let decoder = JSONDecoder()
        let update = try decoder.decode(SessionUpdate.self, from: itemsJson.data(using: .utf8)!)
        let removed = try decoder.decode(SessionUpdate.self, from: removedJson.data(using: .utf8)!)

        XCTAssertEqual(update.sessionUpdateType, "plan_update")
        if case .items(let items) = update.planUpdate?.plan {
            XCTAssertEqual(items.planId, "p1")
            XCTAssertEqual(items.entries.first?.content, "Ship draft support")
        } else {
            XCTFail("Expected item plan update")
        }

        XCTAssertEqual(removed.planRemoved?.planId, "p1")

        let encoded = try JSONSerialization.jsonObject(with: JSONEncoder().encode(update)) as! [String: Any]
        let encodedPlan = encoded["plan"] as? [String: Any]
        XCTAssertEqual(encoded["sessionUpdate"] as? String, "plan_update")
        XCTAssertEqual(encodedPlan?["planId"] as? String, "p1")
    }

    func testDraftProvidersElicitationNesAndDocumentModels() throws {
        let provider = ProviderInfo(
            providerId: "anthropic",
            supported: [.anthropic, .openai],
            required: true,
            current: ProviderCurrentConfig(apiType: .anthropic, baseUrl: "https://api.anthropic.com")
        )
        let elicitation = CreateElicitationRequest(
            mode: "form",
            message: "Choose model",
            sessionId: SessionId("session-1"),
            requestedSchema: ElicitationSchema(
                properties: ["model": AnyCodable(["type": "string"])]
            )
        )
        let suggestion = NesSuggestion(
            kind: "edit",
            id: "sug-1",
            uri: "file:///tmp/main.swift",
            edits: [
                NesTextEdit(
                    range: ACPModel.TextRange(
                        start: TextPosition(line: 1, character: 0),
                        end: TextPosition(line: 1, character: 3)
                    ),
                    newText: "let"
                )
            ]
        )
        let suggestRequest = SuggestNesRequest(
            sessionId: SessionId("session-1"),
            uri: "file:///tmp/main.swift",
            version: 3,
            position: TextPosition(line: 1, character: 4),
            triggerKind: .manual,
            context: NesSuggestContext(
                recentFiles: [
                    NesRecentFile(uri: "file:///tmp/other.swift", languageId: "swift", text: "let other = 1"),
                ],
                diagnostics: [
                    NesDiagnostic(
                        uri: "file:///tmp/main.swift",
                        range: ACPModel.TextRange(
                            start: TextPosition(line: 1, character: 0),
                            end: TextPosition(line: 1, character: 3)
                        ),
                        severity: .warning,
                        message: "Replace var with let"
                    ),
                ]
            )
        )
        let document = DidFocusDocumentNotification(
            sessionId: SessionId("session-1"),
            uri: "file:///tmp/main.swift",
            version: 2,
            position: TextPosition(line: 1, character: 4),
            visibleRange: ACPModel.TextRange(
                start: TextPosition(line: 0, character: 0),
                end: TextPosition(line: 20, character: 0)
            )
        )

        let encoder = JSONEncoder()
        let providerJson = try JSONSerialization.jsonObject(with: encoder.encode(ListProvidersResponse(providers: [provider]))) as! [String: Any]
        let elicitationJson = try JSONSerialization.jsonObject(with: encoder.encode(elicitation)) as! [String: Any]
        let suggestionJson = try JSONSerialization.jsonObject(with: encoder.encode(SuggestNesResponse(suggestions: [suggestion]))) as! [String: Any]
        let suggestRequestJson = try JSONSerialization.jsonObject(with: encoder.encode(suggestRequest)) as! [String: Any]
        let documentJson = try JSONSerialization.jsonObject(with: encoder.encode(document)) as! [String: Any]

        XCTAssertEqual(((providerJson["providers"] as? [[String: Any]])?.first)?["providerId"] as? String, "anthropic")
        XCTAssertEqual(elicitationJson["mode"] as? String, "form")
        XCTAssertEqual(((suggestionJson["suggestions"] as? [[String: Any]])?.first)?["kind"] as? String, "edit")
        XCTAssertEqual(((suggestRequestJson["context"] as? [String: Any])?["diagnostics"] as? [[String: Any]])?.first?["severity"] as? String, "warning")
        XCTAssertEqual(documentJson["visibleRange"] as? [String: Any] != nil, true)
    }

    // MARK: - AnyCodable Tests

    func testAnyCodableWithPrimitives() throws {
        let encoder = JSONEncoder()

        let intValue = AnyCodable(42)
        let intData = try encoder.encode(intValue)
        XCTAssertEqual(String(data: intData, encoding: .utf8), "42")

        let stringValue = AnyCodable("hello")
        let stringData = try encoder.encode(stringValue)
        XCTAssertEqual(String(data: stringData, encoding: .utf8), "\"hello\"")

        let boolValue = AnyCodable(true)
        let boolData = try encoder.encode(boolValue)
        XCTAssertEqual(String(data: boolData, encoding: .utf8), "true")
    }

    func testAnyCodableWithDict() throws {
        let encoder = JSONEncoder()
        let value = AnyCodable(["key": "value", "number": 123] as [String: any Sendable])
        let data = try encoder.encode(value)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["key"] as? String, "value")
        XCTAssertEqual(json["number"] as? Int, 123)
    }

    // MARK: - Request/Response Tests

    func testInitializeRequestEncoding() throws {
        let request = InitializeRequest(
            protocolVersion: 1,
            clientCapabilities: ClientCapabilities(
                fs: FileSystemCapabilities(readTextFile: true, writeTextFile: true),
                terminal: true
            ),
            clientInfo: ClientInfo(name: "Test", title: nil, version: "1.0")
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(request)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["protocolVersion"] as? Int, 1)
        XCTAssertNotNil(json["clientCapabilities"])
        XCTAssertNotNil(json["clientInfo"])
    }

    func testSessionPromptRequestEncoding() throws {
        let request = SessionPromptRequest(
            sessionId: SessionId("session-1"),
            prompt: [.text(TextContent(text: "Hello"))]
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(request)
        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["sessionId"] as? String, "session-1")
        XCTAssertNotNil(json["prompt"])
    }
}
