Feature: DAG Conditional Execution and Observability
  As a workflow author
  I want to conditionally skip DAG nodes and inspect detailed execution reports
  So that dynamic workflows run efficiently with full observability

  Scenario: DAG node is skipped when condition evaluates to false
    Given a DAG with root node "extract"
    And a node "transform" that depends on "extract" with a false condition
    And a node "load" that depends on "transform"
    When the DAG is executed with a report
    Then node "extract" status is "SUCCESS"
    And node "transform" status is "SKIPPED"
    And node "load" status is "SUCCESS"
    And the overall DAG execution succeeds

  Scenario: DAG execution report tracks node latency and errors
    Given a DAG with a failing node "validate"
    And a node "process" that depends on "validate"
    When the DAG is executed with a report
    Then node "validate" status is "FAILED"
    And node "validate" reports an error
    And node "process" is not executed
    And the report records a positive execution duration
