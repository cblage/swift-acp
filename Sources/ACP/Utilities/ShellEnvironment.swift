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
    /// Every load runs here, one at a time, and a caller waits for it with
    /// `sync`: a wait whose owner the system knows, so the load runs at the
    /// priority of the highest caller waiting — never a condition or a
    /// semaphore, which leave a caller waiting on a thread of a lower one.
    private static let loadQueue = DispatchQueue(label: "org.acp.shell-environment")
    /// Both read and written under `cacheLock` only, which is what makes the
    /// unchecked globals sound.
    nonisolated(unsafe) private static var cachedEnvironment: [String: String]?
    /// How many loads have ended, failed ones included: a caller that
    /// arrived before a load ended joined it and takes its answer.
    nonisolated(unsafe) private static var endedLoads = 0

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

    private static func loadState() -> (cached: [String: String]?, ended: Int) {
        cacheLock.lock()
        defer { cacheLock.unlock() }
        return (cachedEnvironment, endedLoads)
    }

    /// Blocking version that waits for environment to be loaded: ONE LOAD AT
    /// A TIME, on `loadQueue`, every caller during it joining it with `sync`
    /// and taking its answer — the shell's environment, or the process's own
    /// where the load failed or timed out. The shell runs with no lock held.
    /// Do NOT call from main thread - use loadUserShellEnvironmentAsync() instead.
    public static func loadUserShellEnvironmentBlocking() -> [String: String] {
        let arrival = loadState()
        if let cached = arrival.cached {
            return cached
        }

        return loadQueue.sync {
            let state = loadState()
            if let cached = state.cached { return cached }
            // A load ended after this caller arrived: it joined that load,
            // which failed, and takes its answer rather than running another.
            if state.ended > arrival.ended { return ProcessInfo.processInfo.environment }

            let loaded = loader()

            cacheLock.lock()
            if let loaded { cachedEnvironment = loaded }
            endedLoads += 1
            cacheLock.unlock()

            return loaded ?? ProcessInfo.processInfo.environment
        }
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
    /// stdin closed, so an interactive shell never waits on input; stdout
    /// read as it arrives and stderr drained, so an environment larger than
    /// a pipe's buffer never blocks the shell before its exit; and past
    /// `timeout` the shell is ended — SIGTERM, then SIGKILL two seconds on —
    /// and nil answers. THE WAITS ARE THE KERNEL'S, never a thread's: `poll`
    /// on the pipe and the process's own state, so a caller at any priority
    /// waits on no thread of a lower one — Foundation's pipe and exit
    /// handlers run at the default priority, below a launch's.
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
        // Drained and dropped, waited on by nobody: left unread, a shell
        // writing enough of it would block before its exit.
        errors.fileHandleForReading.readabilityHandler = { handle in
            if handle.availableData.isEmpty { handle.readabilityHandler = nil }
        }
        defer { errors.fileHandleForReading.readabilityHandler = nil }
        do {
            try process.run()
        } catch {
            return nil
        }

        // The system's uptime, which stops while the Mac sleeps: a load the
        // sleep interrupts keeps the rest of its bound on waking.
        let deadline = ProcessInfo.processInfo.systemUptime + timeout
        let descriptor = output.fileHandleForReading.fileDescriptor
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 65_536)
        var exitedAt: TimeInterval?
        reading: while true {
            let now = ProcessInfo.processInfo.systemUptime
            guard now < deadline else {
                end(process)
                return nil
            }
            var ready = pollfd(fd: descriptor, events: Int16(POLLIN), revents: 0)
            let slice = Int32(min(100, max(1, (deadline - now) * 1000)))
            if poll(&ready, 1, slice) > 0 {
                let count = read(descriptor, &buffer, buffer.count)
                if count == 0 { break reading }
                if count > 0 {
                    data.append(contentsOf: buffer[0..<count])
                } else if errno != EINTR, errno != EAGAIN {
                    break reading
                }
            }
            // The shell gone with its pipe still open: a child it left holds
            // it, writing or not, and a second past the exit what was read
            // is the answer.
            if !process.isRunning {
                if let exitedAt {
                    if now - exitedAt >= 1 { break reading }
                } else {
                    exitedAt = now
                }
            }
        }
        // The output ended; the exit follows within the bound, or the shell
        // is ended.
        while process.isRunning {
            guard ProcessInfo.processInfo.systemUptime < deadline else {
                end(process)
                return nil
            }
            usleep(10_000)
        }

        var environment: [String: String] = [:]
        for line in String(decoding: data, as: UTF8.self).split(separator: "\n") {
            if let equalsIndex = line.firstIndex(of: "=") {
                let key = String(line[..<equalsIndex])
                let value = String(line[line.index(after: equalsIndex)...])
                environment[key] = value
            }
        }
        return environment.isEmpty ? nil : environment
    }

    /// A shell past its bound: SIGTERM, then SIGKILL two seconds on.
    private static func end(_ process: Process) {
        process.terminate()
        let pid = process.processIdentifier
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 2) {
            if process.isRunning { kill(pid, SIGKILL) }
        }
    }

    private static func getLoginShell() -> String {
        if let shell = ProcessInfo.processInfo.environment["SHELL"], !shell.isEmpty {
            return shell
        }

        return "/bin/zsh"
    }
}
#endif
