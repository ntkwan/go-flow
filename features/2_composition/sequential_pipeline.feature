Feature: Sequential Pipeline
  As a workflow author
  I want steps in a sequential pipeline to run one after another
  So that operations execute in strict linear sequence

  Scenario: All sequential steps succeed
    Given 3 sequential steps that record their execution
    When the sequential pipeline is executed
    Then the steps execute in exact order 1, 2, 3
    And the sequential pipeline succeeds

  Scenario: Middle step fails and halts subsequent steps
    Given 3 sequential steps where step 2 fails with "validation error"
    When the sequential pipeline is executed
    Then step 1 executes
    And step 2 executes and fails
    And step 3 is never executed
    And the sequential pipeline fails with "validation error"

  Scenario: Chaining steps with Then
    Given a primary step that succeeds
    And a chained step attached with Then
    When the chained step is executed
    Then both steps execute in sequence
    And the chained execution succeeds
