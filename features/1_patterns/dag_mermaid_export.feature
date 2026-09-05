Feature: DAG Mermaid Export
  As a developer
  I want to export DAG workflows to Mermaid markdown syntax
  So that execution graphs can be visualized and self-documented

  Scenario: Export connected DAG to Mermaid diagram
    Given a DAG with dependency between "fetch-user" and "process-payment"
    When the DAG is exported to Mermaid format
    Then the Mermaid output contains edge from "fetch-user" to "process-payment"
    And the Mermaid output starts with "graph TD"

  Scenario: Export isolated node to Mermaid diagram
    Given a DAG with an isolated node "health-check"
    When the DAG is exported to Mermaid format
    Then the Mermaid output contains node "health-check"
