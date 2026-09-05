Feature: Dynamic Step Routing
  As a workflow author
  I want runtime dynamic step selection
  So that pipeline steps can be resolved and executed lazily based on context state

  Scenario: Dynamic factory returns and executes a successful step
    Given a dynamic step factory resolving to a successful branch
    When the dynamic step is executed
    Then the selected branch executes
    And the dynamic execution succeeds

  Scenario: Dynamic factory returns a step that fails
    Given a dynamic step factory resolving to a failing branch with "dynamic error"
    When the dynamic step is executed
    Then the dynamic execution fails with "dynamic error"

  Scenario: Dynamic factory returns a nil step
    Given a dynamic step factory returning nil
    When the dynamic step is executed
    Then the dynamic execution succeeds as a no-op

  Scenario: Dynamic combinator provided with nil factory
    Given a dynamic combinator with nil factory
    When the dynamic step is executed
    Then the dynamic execution succeeds as a no-op

  Scenario: Fluent Dynamic method chaining executes sequentially
    Given a primary step chained with fluent Dynamic step
    When the fluent dynamic step is executed
    Then both the primary step and dynamic step execute in order
    And the fluent dynamic execution succeeds
