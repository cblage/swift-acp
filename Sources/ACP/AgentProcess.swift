//
//  AgentProcess.swift
//  ACP
//
//  One agent's process for a client with a wire of its own
//

#if os(macOS)
import Foundation

/// An agent process's end: its exit status — the code it exited with, or
/// the number of the signal that ended it — and the newest of what it
/// wrote on stderr.
public struct AgentExit: Sendable, Equatable {
    public let status: Int32
    public let stderrTail: String

    public init(status: Int32, stderrTail: String) {
        self.status = status
        self.stderrTail = stderrTail
    }
}

/// ONE AGENT'S PROCESS FOR A CLIENT WITH A WIRE OF ITS OWN, on the process
/// layer every `Client` runs on: the same launch — the shell's environment
/// under the caller's, the working directory, the agent's own folder first
/// on `PATH`, a Node script run under the `node` found beside it — the same
/// process group, registry, stdin, and stderr, and the same end
/// (`terminate(grace:)`), with stdout handed on as the bytes the agent
/// wrote, in order, for the caller to frame.
public final class AgentProcess: Sendable {
    private let manager: ACPProcessManager

    /// `qos` is the intake's band, as a `Client`'s is.
    public init(qos: DispatchQoS = .unspecified) {
        manager = ACPProcessManager(qos: qos)
    }

    /// Spawns the agent: every chunk of its stdout to `onStdout` on the
    /// read queue, in order and unframed, and its exit's status to `onEnd`
    /// once every chunk before it was handed on.
    public func launch(
        agentPath: String,
        arguments: [String] = [],
        workingDirectory: String? = nil,
        environment: [String: String]? = nil,
        onStdout: @escaping @Sendable (Data) -> Void,
        onEnd: @escaping @Sendable (Int32) -> Void
    ) async throws {
        manager.setRawHandlers(onStdout: onStdout, onTermination: { status in onEnd(status) })
        try await manager.launch(
            agentPath: agentPath, arguments: arguments, workingDirectory: workingDirectory,
            environment: environment)
    }

    /// The bytes as they are to stdin, after every write before them, from
    /// any thread; `completion` gets nil once written, or the failure.
    public func write(_ data: Data, completion: @escaping @Sendable (Error?) -> Void) {
        manager.write(data, completion: completion)
    }

    /// Every client's end (`Client.terminate`): stdin closed behind the
    /// writes before it, then — past `grace` for the agent's own end at
    /// that close — SIGTERM to its group, then SIGKILL; the group takes a
    /// SIGTERM even where the agent ended within the grace. Returns once
    /// the process has exited or the kill is sent.
    public func terminate(grace: TimeInterval = 0) async {
        await manager.terminate(grace: grace)
    }

    /// The newest of what the agent wrote on stderr.
    public var stderrTail: String {
        manager.stderrTail
    }

    /// The agent's exit, once it has ended.
    public var exit: AgentExit? {
        manager.lastExit
    }
}
#endif
