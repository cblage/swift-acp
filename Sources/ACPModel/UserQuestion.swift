// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. 
// If a copy of the MPL was not distributed with this file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

import Foundation

public struct UserQuestionRequest: Codable, Sendable {
    public let sessionId: SessionId
    public let questions: [UserQuestion]
    public let metadata: AnyCodable?
    public let actions: [UserQuestionAction]

    public init(
        sessionId: SessionId,
        questions: [UserQuestion],
        metadata: AnyCodable? = nil,
        actions: [UserQuestionAction] = []
    ) {
        self.sessionId = sessionId
        self.questions = questions
        self.metadata = metadata
        self.actions = actions
    }
}

public struct UserQuestion: Codable, Sendable {
    public let header: String?
    public let question: String
    public let options: [UserQuestionOption]
    public let multiSelect: Bool

    public init(header: String? = nil, question: String, options: [UserQuestionOption], multiSelect: Bool = false) {
        self.header = header
        self.question = question
        self.options = options
        self.multiSelect = multiSelect
    }
}

public struct UserQuestionOption: Codable, Sendable {
    public let label: String
    public let description: String?

    public init(label: String, description: String? = nil) {
        self.label = label
        self.description = description
    }
}

public struct UserQuestionAction: Codable, Sendable {
    public let kind: String
    public let name: String
    public let id: String

    public init(kind: String, name: String, id: String) {
        self.kind = kind
        self.name = name
        self.id = id
    }
}

public struct UserQuestionOutcome: Codable, Sendable {
    public let outcome: String
    public let optionId: String?

    public init(optionId: String) {
        self.outcome = "selected"
        self.optionId = optionId
    }

    public init() {
        self.outcome = "cancelled"
        self.optionId = nil
    }
}

public struct UserQuestionResponse: Codable, Sendable {
    public let outcome: UserQuestionOutcome
    public let answers: [String: String]?

    public init(outcome: UserQuestionOutcome, answers: [String: String]? = nil) {
        self.outcome = outcome
        self.answers = answers
    }
}
