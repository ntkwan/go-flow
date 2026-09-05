Feature: Parallel Fan-out
  As a workflow author
  I want to run multiple steps in parallel
  So that independent operations complete concurrently

  Scenario: All parallel steps succeed
    Given 3 parallel steps that succeed
    When the parallel flow is executed
    Then all 3 steps complete
    And the parallel flow succeeds

  Scenario: One parallel step fails
    Given 2 parallel steps that succeed and 1 that fails with "network timeout"
    When the parallel flow is executed
    Then the parallel flow fails with error containing "network timeout"

  Scenario: Multiple parallel steps fail
    Given 2 parallel steps that fail with errors "db error" and "cache error"
    When the parallel flow is executed
    Then the parallel flow fails with joined error containing "db error" and "cache error"

  Scenario: Bounded concurrency respects limit
    Given 5 parallel steps with a concurrency limit of 2
    When the bounded parallel flow is executed
    Then at most 2 steps run concurrently
    And the bounded parallel flow succeeds
