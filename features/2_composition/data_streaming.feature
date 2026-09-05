Feature: Data Streaming Pipeline
  As a workflow author
  I want typed pipe streaming
  So that data items are transformed and consumed cleanly in sequence

  Scenario: Pipe transforms typed input to output in context
    Given a typed pipe step that multiplies input by 2
    When the pipe step is executed with input 21
    Then the output in context is "42"
    And the pipe execution succeeds

  Scenario: PipeSeq streams and transforms items
    Given a stream of 5 integers from 1 to 5
    When the stream is piped through double transform
    Then 5 transformed items are collected in order
    And the pipe stream execution succeeds
