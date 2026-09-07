//
//  JSONCoding.swift
//  ACPModel
//
//  The package's JSON coders, wrapped so no failure is silent
//

import Foundation
import YYJSON
import os.log

/// THE PACKAGE'S JSON CODERS: yyjson's, wrapped so that NO FAILURE IS SILENT.
/// Every decode, encode, and serialization in the package goes through one of
/// these, and one that fails logs the type, the direction, the error, and a
/// bounded excerpt of the bytes at error level, then throws exactly as the
/// wrapped coder did — a `try?` site that swallowed the error keeps swallowing
/// it, and the log is what it never had. Two sessions froze mid-turn with no
/// word from anything; a frame the decoder refused was the one thing that
/// could do that without a trace.
public enum ACPJSONLog {
    public static let logger = Logger(subsystem: "com.acp", category: "JSON")

    /// The first bytes of the payload, enough to say which message it was.
    static func excerpt(_ data: Data, limit: Int = 400) -> String {
        let head = String(decoding: data.prefix(limit), as: UTF8.self)
        return data.count > limit ? "\(head)… (\(data.count) bytes)" : head
    }
}

/// A `YYJSONDecoder` that logs what it refuses. A value type of options, like
/// the coder it wraps, and not `Sendable` for the same reason: each actor
/// builds its own.
public struct ACPJSONDecoder {
    private let decoder = YYJSONDecoder()

    public init() {}

    public func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        do {
            return try decoder.decode(type, from: data)
        } catch {
            ACPJSONLog.logger.error(
                "JSON decode of \(String(describing: type), privacy: .public) failed: \(String(describing: error), privacy: .public) — \(ACPJSONLog.excerpt(data), privacy: .public)"
            )
            throw error
        }
    }
}

/// A `YYJSONEncoder` that logs what it cannot encode. Escapes no slash, as the
/// coder it wraps does by default.
public struct ACPJSONEncoder {
    private let encoder = YYJSONEncoder()

    public init() {}

    public func encode<T: Encodable>(_ value: T) throws -> Data {
        do {
            return try encoder.encode(value)
        } catch {
            ACPJSONLog.logger.error(
                "JSON encode of \(String(describing: T.self), privacy: .public) failed: \(String(describing: error), privacy: .public)"
            )
            throw error
        }
    }
}

/// `YYJSONSerialization` with the same logging on both directions.
public enum ACPJSONSerialization {
    public static func jsonObject(with data: Data) throws -> Any {
        do {
            return try YYJSONSerialization.jsonObject(with: data)
        } catch {
            ACPJSONLog.logger.error(
                "JSON parse to object failed: \(String(describing: error), privacy: .public) — \(ACPJSONLog.excerpt(data), privacy: .public)"
            )
            throw error
        }
    }

    public static func data(withJSONObject object: Any) throws -> Data {
        do {
            return try YYJSONSerialization.data(withJSONObject: object)
        } catch {
            ACPJSONLog.logger.error(
                "JSON write from object failed: \(String(describing: error), privacy: .public) — \(String(describing: type(of: object)), privacy: .public)"
            )
            throw error
        }
    }
}
