Feature: Idempotency gate
  Spec: docs/specs/spec-idempotency-gate.md

  Scenario: same idempotency_key resent is rejected and creates no new row
    Given a first transaction accepted through the gate
    When the same idempotency_key is resent
    Then the retry is rejected with 409
    And no new row is created in txn-idempotency

  Scenario: 20 concurrent requests with the same new idempotency_key
    Given a brand-new idempotency_key
    When 20 concurrent requests are sent with that key
    Then exactly 1 request is accepted with 200
    And exactly 19 requests are rejected with 409

  Scenario: a late retry one second later is still rejected
    Given a first transaction accepted through the gate
    When a retry with the same key arrives 1 second later
    Then the retry is rejected with 409
