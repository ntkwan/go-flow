Feature: Error Isolation and Panic Handling
  As a workflow author
  I want errors and panics to be isolated and controlled
  So that crashes are avoided and errors are handled cleanly

  Scenario: Error suppressed by Catch handler
    Given a step that fails with "ignored warning"
    And a Catch handler that suppresses the error
    When the guarded step is executed
    Then the guarded execution succeeds

  Scenario: Error transformed by Catch handler
    Given a step that fails with "low-level db error"
    And a Catch handler that transforms it to "internal service error"
    When the guarded step is executed
    Then the execution fails with "internal service error"

  Scenario: Panic recovered into error
    Given a step that panics with "unexpected nil pointer"
    And a Recover guard attached to the step
    When the guarded step is executed
    Then the execution fails with a panic error containing "unexpected nil pointer"
    And no panic crashes the process

  Scenario: Multiple concurrent errors joined
    Given 2 concurrent steps failing with "disk full" and "permission denied"
    When the multi-error flow is executed
    Then the resulting error unwraps to "disk full"
    And the resulting error unwraps to "permission denied"
