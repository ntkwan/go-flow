Feature: Conditional Branching
  As a workflow author
  I want conditional branching logic
  So that steps execute selectively based on runtime conditions

  Scenario: Condition is true executes if-branch
    Given a conditional branch with true condition
    When the conditional branch is executed
    Then the if-branch executes
    And the else-branch is never executed
    And the branch execution succeeds

  Scenario: Condition is false executes else-branch
    Given a conditional branch with false condition
    When the conditional branch is executed
    Then the else-branch executes
    And the if-branch is never executed
    And the branch execution succeeds
