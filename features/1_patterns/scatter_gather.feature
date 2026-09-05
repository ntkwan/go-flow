Feature: Scatter-Gather Pattern
  As a workflow author
  I want to scatter work across parallel steps and gather results
  So that aggregated data is available to downstream steps

  Scenario: Scatter tasks across workers and gather results
    Given 3 scatter steps producing results "alpha", "beta", "gamma"
    And a gather step that collects all results into an aggregate
    When the scatter-gather workflow is executed
    Then the aggregate contains "alpha", "beta", "gamma"
    And the scatter-gather workflow succeeds
