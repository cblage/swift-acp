import XCTest
@testable import ACP

final class FileSystemDelegateTests: XCTestCase {
    func testBoundedReadsClampOffsetsAndLimits() async throws {
        let content = "one\ntwo\nthree\nfour"
        let cases: [(line: Int, limit: Int, expected: String)] = [
            (1, 2, "one\ntwo"),
            (2, 2, "two\nthree"),
            (4, 10, "four"),
            (5, 1, ""),
            (10, 2, ""),
            (Int.max, 1, ""),
            (Int.max, Int.max, ""),
            (Int.max, Int.min, ""),
            (0, 1, "one"),
            (-1, 1, "one"),
            (Int.min, 1, "one"),
            (Int.min, Int.max, content),
            (2, Int.max, "two\nthree\nfour"),
            (2, 0, ""),
            (2, -1, ""),
            (2, Int.min, ""),
            (Int.min, Int.min, "")
        ]
        let file = try temporaryFile(content)
        defer { try? FileManager.default.removeItem(at: file) }
        let delegate = FileSystemDelegate()

        for test in cases {
            let response = try await delegate.handleFileReadRequest(
                file.path, sessionId: "s1", line: test.line, limit: test.limit)
            let context = "line=\(test.line), limit=\(test.limit)"
            XCTAssertEqual(response.content, test.expected, context)
            XCTAssertEqual(response.totalLines, 4, context)
        }
    }

    func testSuffixReadsClampOffsets() async throws {
        let content = "one\ntwo\nthree\nfour"
        let cases: [(line: Int, expected: String)] = [
            (1, content),
            (2, "two\nthree\nfour"),
            (4, "four"),
            (5, ""),
            (10, ""),
            (Int.max, ""),
            (0, content),
            (-1, content),
            (Int.min, content)
        ]
        let file = try temporaryFile(content)
        defer { try? FileManager.default.removeItem(at: file) }
        let delegate = FileSystemDelegate()

        for test in cases {
            let response = try await delegate.handleFileReadRequest(
                file.path, sessionId: "s1", line: test.line, limit: nil)
            XCTAssertEqual(response.content, test.expected, "line=\(test.line)")
            XCTAssertEqual(response.totalLines, 4, "line=\(test.line)")
        }
    }

    func testEmptyFilesAndTrailingNewlines() async throws {
        let cases: [(content: String, line: Int?, limit: Int?, expected: String, total: Int)] = [
            ("", nil, nil, "", 1),
            ("", 1, 1, "", 1),
            ("", 2, nil, "", 1),
            ("", Int.max, Int.max, "", 1),
            ("one\ntwo\n", nil, nil, "one\ntwo\n", 3),
            ("one\ntwo\n", 2, nil, "two\n", 3),
            ("one\ntwo\n", 2, 1, "two", 3),
            ("one\ntwo\n", 2, Int.max, "two\n", 3),
            ("one\ntwo\n", 3, 1, "", 3),
            ("one\ntwo\n", 4, nil, "", 3)
        ]
        let delegate = FileSystemDelegate()

        for test in cases {
            let file = try temporaryFile(test.content)
            defer { try? FileManager.default.removeItem(at: file) }
            let response = try await delegate.handleFileReadRequest(
                file.path, sessionId: "s1", line: test.line, limit: test.limit)
            let context = "content=\(test.content.debugDescription), line=\(String(describing: test.line)), limit=\(String(describing: test.limit))"
            XCTAssertEqual(response.content, test.expected, context)
            XCTAssertEqual(response.totalLines, test.total, context)
        }
    }

    func testWholeFileReadsPreserveNewlinesAndIgnoreLimit() async throws {
        let content = "one\r\ntwo\rthree\nfour\n"
        let limits: [Int?] = [nil, 1, 0, -1, Int.min, Int.max]
        let file = try temporaryFile(content)
        defer { try? FileManager.default.removeItem(at: file) }
        let delegate = FileSystemDelegate()

        for limit in limits {
            let response = try await delegate.handleFileReadRequest(
                file.path, sessionId: "s1", line: nil, limit: limit)
            XCTAssertEqual(response.content, content, "limit=\(String(describing: limit))")
            XCTAssertEqual(response.totalLines, 6, "limit=\(String(describing: limit))")
        }
    }

    private func temporaryFile(_ content: String) throws -> URL {
        let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try content.write(to: file, atomically: true, encoding: .utf8)
        return file
    }
}
