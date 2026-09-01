Feature: Schema validation and quarantine
  Spec: docs/specs/spec-schema-validation-quarantine.md

  Scenario: schema_version that does not match the registry is invalid
    Given a transaction event with schema_version 99
    When the event is checked against registry version 1
    Then the event is rejected
    And the reason mentions schema_version mismatch

  Scenario: negative amount_cents is invalid
    Given a transaction event with amount_cents -500
    When the event is checked against registry version 1
    Then the event is rejected
    And the reason mentions positive integer

  Scenario: missing required field is invalid
    Given a transaction event missing amount_cents
    When the event is checked against registry version 1
    Then the event is rejected
    And the reason mentions missing fields

  Scenario: a complete valid event is accepted
    Given a complete valid transaction event
    When the event is checked against registry version 1
    Then the event is accepted
