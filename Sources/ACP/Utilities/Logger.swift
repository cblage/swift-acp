//
//  Logger+ACP.swift
//  ACP
//
//  Logging utility for ACP
//

import Foundation
import os.log

extension Logger {
    /// Default subsystem for ACP logging. Read and written under
    /// `subsystemLock`, which is what makes the unchecked global sound: the
    /// setter is the app's, at launch, and every logger created after reads
    /// the value it set.
    nonisolated(unsafe) private static var acpSubsystem = "com.acp"
    private static let subsystemLock = NSLock()

    /// Configure the logging subsystem (call once at initialization)
    public static func configureACPLogging(subsystem: String) {
        subsystemLock.lock()
        acpSubsystem = subsystem
        subsystemLock.unlock()
    }

    /// Create a logger for a specific category
    public static func forCategory(_ category: String) -> Logger {
        subsystemLock.lock()
        let subsystem = acpSubsystem
        subsystemLock.unlock()
        return Logger(subsystem: subsystem, category: category)
    }

    /// Convenience logger for ACP
    public static let acp = Logger.forCategory("ACP")
}
