Feature: Step Aggregation
  As a workflow author
  I want concurrent steps to write partial state into context
  So that aggregated state is available to subsequent steps

  Scenario: Aggregate context writes from parallel steps
    Given 4 concurrent steps writing their partition keys to context
    When the parallel aggregation step is executed
    Then all 4 partition keys are present in context
    And the aggregation step succeeds
