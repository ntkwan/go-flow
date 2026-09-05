Feature: DAG Execution
  As a workflow author
  I want DAG steps to execute in topological dependency order
  So that dependent operations only run after prerequisites succeed

  Scenario: Nodes execute in dependency order
    Given a DAG with root node "A"
    And a node "B" that depends on "A"
    And a node "C" that depends on "B"
    When the DAG is executed
    Then node "A" completes before "B" starts
    And node "B" completes before "C" starts
    And the DAG execution succeeds

  Scenario: DAG halts downstream nodes on failure
    Given a DAG with root node "A" that returns an error
    And a node "B" that depends on "A"
    When the DAG is executed
    Then node "B" is never executed
    And the DAG execution fails with node "A" error

  Scenario: Independent nodes execute concurrently
    Given a DAG with independent nodes "A" and "B"
    When the DAG is executed
    Then nodes "A" and "B" execute concurrently
    And the DAG execution succeeds
