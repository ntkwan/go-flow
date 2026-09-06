Feature: Error Isolation and Panic Handling
  As a workflow author
  I want errors and panics to be isolated and controlled
  So that crashes are avoided and errors are handled cleanly

  Scenario: Panic recovered into error via Recovery middleware
    Given a step that panics with "middleware runtime panic"
    And a Recovery middleware wrapping the step
    When the guarded step is executed
    Then the execution fails with a panic error containing "middleware runtime panic"
    And no panic crashes the process

  Scenario: Multiple concurrent errors joined
    Given 2 concurrent steps failing with "disk full" and "permission denied"
    When the multi-error flow is executed
    Then the resulting error unwraps to "disk full"
    And the resulting error unwraps to "permission denied"
