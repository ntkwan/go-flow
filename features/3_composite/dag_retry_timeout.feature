Feature: DAG with Retry and Timeout
  As a workflow author
  I want DAG nodes to support retry and timeout resilience
  So that transient failures are handled within bounded time

  Scenario: DAG node succeeds on retry attempt
    Given a DAG node configured to retry up to 3 times
    And the node fails on attempt 1 and succeeds on attempt 2
    When the resilient DAG is executed
    Then the node executes 2 times
    And the resilient DAG execution succeeds

  Scenario: DAG node exhausts retries and fails DAG
    Given a DAG node configured to retry up to 2 times
    And the node fails on every attempt with "persistent error"
    When the resilient DAG is executed
    Then the node executes 2 times total
    And the resilient DAG execution fails with "persistent error"

  Scenario: DAG node timeout exceeds limit
    Given a DAG node configured with a 50 millisecond timeout
    And the node step takes 200 milliseconds to complete
    When the resilient DAG is executed
    Then the resilient DAG execution fails with deadline exceeded error
