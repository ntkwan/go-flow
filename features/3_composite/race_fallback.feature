Feature: Speculative Race with Fallback
  As a workflow author
  I want to race multiple steps and provide fallback on total failure
  So that latency is minimized and resilience is preserved

  Scenario: First racing step wins and cancels losing branches
    Given 3 racing steps where step 1 finishes in 10ms, step 2 in 100ms, and step 3 in 200ms
    When the racing steps are executed
    Then the fastest step wins
    And losing branches are cancelled
    And the race execution succeeds

  Scenario: All racing steps fail
    Given 3 racing steps that all fail with error "endpoint unavailable"
    When the racing steps are executed
    Then the race execution fails

  Scenario: Race fails and invokes fallback step
    Given 3 racing steps that all fail with error "upstream failure"
    And a race fallback step that succeeds
    When the racing steps with fallback are executed
    Then the race fallback step executes
    And the race execution with fallback succeeds
