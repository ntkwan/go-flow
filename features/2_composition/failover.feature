Feature: Failover and Fallback
  As a workflow author
  I want to define fallback steps for failure cases
  So that workflows can recover smoothly when primary steps fail

  Scenario: Primary step fails and fallback succeeds
    Given a primary step that fails with "primary service down"
    And a fallback step that succeeds on failover
    When the step with fallback is executed
    Then the fallback step executes
    And the failover execution succeeds

  Scenario: Primary step succeeds and fallback is bypassed
    Given a primary step that succeeds
    And a fallback step is attached
    When the step with fallback is executed
    Then the fallback step is never executed
    And the failover execution succeeds
