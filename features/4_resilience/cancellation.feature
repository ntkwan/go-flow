Feature: Context Cancellation and Timeout
  As a workflow author
  I want workflows to respect context cancellation and timeouts
  So that abandoned or slow executions are cleanly stopped

  Scenario: Step immediately aborts on pre-cancelled context
    Given a context that is already cancelled
    And a step checking context cancellation
    When the step is executed with the cancelled context
    Then the step aborts immediately with context cancelled error

  Scenario: Step times out during execution
    Given a step configured with a 30 millisecond timeout
    And the step operation takes 150 milliseconds
    When the timed step is executed
    Then the step returns deadline exceeded error
