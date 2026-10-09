//
//  ShellEnvironment.swift
//  ACP
//
//  Shell environment loading utility
//

#if os(macOS)
import Foundation
import os.log

public enum ShellEnvironment: Sendable {
    private static let cacheLock = NSLock()
    private static let cacheCondition = NSCondition()
    /// Both read and written under `cacheLock` only — the waiting reader
    /// below takes it through `loadState` between its condition waits —
    /// which is what makes the unchecked globals sound.
    nonisolated(unsafe) private static var cachedEnvironment: [String: String]?
    nonisolated(unsafe) private static var isLoading = false

    /// How long a load waits for the login shell before it ends it. A shell
    /// past it — a startup file waiting on something that never comes —
    /// answers with the process's own environment, nothing is cached, and
    /// the next load runs the shell again.
    nonisolated(unsafe) public static var loadTimeout: TimeInterval = 30

    /// The shell run a load makes; a test replaces it.
    nonisolated(unsafe) static var loader: @Sendable () -> [String: String]? = {
        loadEnvironmentFromShell()
    }

    /// Whether the login shell's environment has loaded and is cached: false
    /// before the first load ends, and after one that failed or timed out,
    /// whose callers were answered with the process's own environment.
    public static var isLoaded: Bool { loadState().cached != nil }

    /// The user's shell environment, cached after the first load. On the
    /// main thread it never waits on a shell: the process's own environment
    /// answers at once and the load starts in the background — use
    /// `loadUserShellEnvironmentAsync()` for the complete one. Anywhere else
    /// it joins the one load, as `loadUserShellEnvironmentBlocking()` does.
    public static func loadUserShellEnvironment() -> [String: String] {
        if let cached = loadState().cached {
            return cached
        }
        if Thread.isMainThread {
            DispatchQueue.global(qos: .utility).async {
                _ = loadUserShellEnvironmentBlocking()
            }
            return ProcessInfo.processInfo.environment
        }
        return loadUserShellEnvironmentBlocking()
    }

    /// Async version that guarantees the full user shell environment is loaded.
    /// Safe to call from any context (main thread, actors, etc.)
    public static func loadUserShellEnvironmentAsync() async -> [String: String] {
        if let cached = loadState().cached {
            return cached
        }

        return await withCheckedContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                let env = loadUserShellEnvironmentBlocking()
                continuation.resume(returning: env)
            }
        }
    }

    private static func loadState() -> (cached: [String: String]?, loading: Bool) {
        cacheLock.lock()
        defer { cacheLock.unlock() }
        return (cachedEnvironment, isLoading)
    }

    /// Blocking version that waits for environment to be loaded: ONE LOAD AT
    /// A TIME, every caller during it joining it and taking its answer — the
    /// shell's environment, or the process's own where the load failed or
    /// timed out. The shell runs with no lock held.
    /// Do NOT call from main thread - use loadUserShellEnvironmentAsync() instead.
    public static func loadUserShellEnvironmentBlocking() -> [String: String] {
        cacheLock.lock()

        if let cached = cachedEnvironment {
            cacheLock.unlock()
            return cached
        }

        if isLoading {
            cacheLock.unlock()
            // The state is read under the condition's lock, which the
            // loader broadcasts under, so no wakeup falls between the read
            // and the wait.
            cacheCondition.lock()
            defer { cacheCondition.unlock() }
            while true {
                let state = loadState()
                if let cached = state.cached { return cached }
                if !state.loading { return ProcessInfo.processInfo.environment }
                cacheCondition.wait()
            }
        }

        isLoading = true
        cacheLock.unlock()

        let loaded = loader()

        cacheLock.lock()
        if let loaded { cachedEnvironment = loaded }
        isLoading = false
        cacheLock.unlock()

        cacheCondition.lock()
        cacheCondition.broadcast()
        cacheCondition.unlock()

        return loaded ?? ProcessInfo.processInfo.environment
    }

    /// Preload environment in background (call at app launch)
    public static func preloadEnvironment() {
        DispatchQueue.global(qos: .userInitiated).async {
            _ = loadUserShellEnvironment()
        }
    }

    /// Force reload of environment (e.g., after user changes shell config)
    public static func reloadEnvironment() {
        cacheLock.lock()
        cachedEnvironment = nil
        cacheLock.unlock()
        preloadEnvironment()
    }

    /// The cache emptied and the loader restored, for a test that replaced it.
    static func resetForTesting() {
        cacheLock.lock()
        cachedEnvironment = nil
        isLoading = false
        cacheLock.unlock()
        loader = { loadEnvironmentFromShell() }
    }

    /// The login shell's environment, or nil where the shell could not run,
    /// printed nothing, or did not finish within `loadTimeout`.
    static func loadEnvironmentFromShell() -> [String: String]? {
        let shell = getLoginShell()
        let shellName = (shell as NSString).lastPathComponent
        let arguments: [String]
        switch shellName {
        case "fish":
            arguments = ["-l", "-c", "env"]
        case "zsh", "bash":
            arguments = ["-l", "-i", "-c", "env"]
        case "sh":
            arguments = ["-l", "-c", "env"]
        default:
            arguments = ["-c", "env"]
        }
        return run(shell: shell, arguments: arguments, timeout: loadTimeout)
    }

    /// Runs `shell` in the home directory and reads the `env` it prints:
    /// stdin closed, so an interactive shell never waits on input; stdout and
    /// stderr read as they arrive, so an environment larger than a pipe's
    /// buffer never blocks the shell before its exit; and past `timeout` the
    /// shell is ended — SIGTERM, then SIGKILL two seconds on — and nil
    /// answers.
    static func run(shell: String, arguments: [String], timeout: TimeInterval) -> [String: String]? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: shell)
        process.arguments = arguments
        process.currentDirectoryURL = FileManager.default.homeDirectoryForCurrentUser
        process.standardInput = FileHandle.nullDevice

        let output = Pipe()
        let errors = Pipe()
        process.standardOutput = output
        process.standardError = errors
        let collected = CollectedOutput()
        output.fileHandleForReading.readabilityHandler = { handle in
            let data = handle.availableData
            if data.isEmpty {
                handle.readabilityHandler = nil
                collected.ended.signal()
            } else {
                collected.append(data)
            }
        }
        // Drained and dropped: left unread, a shell writing enough of it
        // would block before its exit.
        errors.fileHandleForReading.readabilityHandler = { handle in
            if handle.availableData.isEmpty { handle.readabilityHandler = nil }
        }
        let exited = DispatchSemaphore(value: 0)
        process.terminationHandler = { _ in exited.signal() }

        func stopReading() {
            output.fileHandleForReading.readabilityHandler = nil
            errors.fileHandleForReading.readabilityHandler = nil
        }
        do {
            try process.run()
        } catch {
            stopReading()
            return nil
        }
        guard exited.wait(timeout: .now() + timeout) == .success else {
            process.terminate()
            let pid = process.processIdentifier
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 2) {
                if process.isRunning { kill(pid, SIGKILL) }
            }
            stopReading()
            return nil
        }
        // The rest of stdout, a second at most: a child the shell left
        // holding the pipe keeps no load waiting.
        _ = collected.ended.wait(timeout: .now() + 1)
        stopReading()

        var environment: [String: String] = [:]
        for line in String(decoding: collected.data, as: UTF8.self).split(separator: "\n") {
            if let equalsIndex = line.firstIndex(of: "=") {
                let key = String(line[..<equalsIndex])
                let value = String(line[line.index(after: equalsIndex)...])
                environment[key] = value
            }
        }
        return environment.isEmpty ? nil : environment
    }

    private static func getLoginShell() -> String {
        if let shell = ProcessInfo.processInfo.environment["SHELL"], !shell.isEmpty {
            return shell
        }

        return "/bin/zsh"
    }
}

/// What a shell's stdout has written so far, and its end.
private final class CollectedOutput: @unchecked Sendable {
    private let lock = NSLock()
    private var buffer = Data()
    let ended = DispatchSemaphore(value: 0)

    func append(_ data: Data) {
        lock.lock()
        buffer.append(data)
        lock.unlock()
    }

    var data: Data {
        lock.lock()
        defer { lock.unlock() }
        return buffer
    }
}
#endif
