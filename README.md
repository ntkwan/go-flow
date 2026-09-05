# go-flow

[![CI](https://github.com/ntkwan/go-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/ntkwan/go-flow/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ntkwan/go-flow)](https://goreportcard.com/report/github.com/ntkwan/go-flow)
[![Go Reference](https://pkg.go.dev/badge/github.com/ntkwan/go-flow.svg)](https://pkg.go.dev/github.com/ntkwan/go-flow)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A high-performance, minimalist Go workflow and step execution engine designed for modern Go (1.27+). Built with generics, standard iterators, and zero external dependencies.

## Features

- **Generic Step Abstraction**: `Step[T context.Context]` operates natively on standard contexts and custom context types.
- **Fluent Receiver Methods**: Chain, compose, and harden steps with `.Then()`, `.Go()`, `.GoN()`, `.Race()`, `.Once()`, `.Timeout()`, `.Retry()`, `.Fallback()`, `.Catch()`, `.Recover()`, `.When()`, `.Unless()`.
- **Sequential Execution (`Seq`)**: Executes steps in strict order, aborting immediately on the first error.
- **Unbounded Concurrency (`Go`)**: Executes steps concurrently across goroutines, joining all errors.
- **Bounded Concurrency (`GoN`)**: Throttles concurrent step execution with an exact worker pool / concurrency limit.
- **Speculative Racing (`Race`)**: Races steps concurrently, returning immediately on first success while canceling losing branches.
- **Iterator Streaming (`Each`, `Each2`)**: Streams Go `iter.Seq` and `iter.Seq2` sequences directly through step pipelines.
- **Idempotent Step (`Once`)**: Guarantees a step executes at most once across workflows and branches.
- **Topological DAG Engine (`DAG`, `Node`)**: Declarative directed acyclic graph execution with cycle validation and maximal branch concurrency.
- **Zero Dependencies**: 100% standard library.

## Installation

```bash
go get github.com/ntkwan/go-flow
```

## Quick Start

### 1. Fluent Method Chaining

Every `Step[T]` provides fluent receiver methods for composition, conditional execution, and resilience:

```go
validateInput := flow.Step[context.Context](validateStep)
fetchUserData := flow.Step[context.Context](fetchUserStep)
updateCache   := flow.Step[context.Context](updateCacheStep)

pipeline := validateInput.
	Then(fetchUserData.Retry(3, 100*time.Millisecond).Timeout(2*time.Second)).
	Then(updateCache.Once()).
	Recover()

if err := pipeline.Exec(ctx); err != nil {
	log.Println("Pipeline failed:", err)
}
```

### 2. Sequential Execution (`Seq`)

```go
package main

import (
	"context"
	"fmt"

	"github.com/ntkwan/go-flow"
)

func main() {
	workflow := flow.Seq(
		func(ctx context.Context) error {
			fmt.Println("Step 1: Validate input")
			return nil
		},
		func(ctx context.Context) error {
			fmt.Println("Step 2: Save to database")
			return nil
		},
	)

	if err := workflow(context.Background()); err != nil {
		panic(err)
	}
}
```

### 3. Parallel Execution (`Go`)

```go
workflow := flow.Go(
	func(ctx context.Context) error { return fetchUserProfile(ctx) },
	func(ctx context.Context) error { return fetchUserOrders(ctx) },
	func(ctx context.Context) error { return fetchUserPreferences(ctx) },
)

if err := workflow(ctx); err != nil {
	// Returns errors joined via errors.Join
	log.Println("Workflow errors:", err)
}
```

### 4. Bounded Concurrency (`GoN`)

```go
var steps []flow.Step[context.Context]
for _, item := range batchItems {
	steps = append(steps, func(ctx context.Context) error {
		return processItem(ctx, item)
	})
}

// Concurrently process items with at most 5 active workers
pipeline := flow.GoN(5, steps...)
if err := pipeline(ctx); err != nil {
	log.Println("Processing failed:", err)
}
```

### 5. Speculative Racing (`Race`)

```go
// Query multiple replicas or fallbacks; returns on first success and cancels the others
fastest := flow.Race(
	func(ctx context.Context) error { return queryPrimaryServer(ctx) },
	func(ctx context.Context) error { return queryMirrorServer(ctx) },
	func(ctx context.Context) error { return queryCachedReplica(ctx) },
)

if err := fastest(ctx); err != nil {
	log.Println("All endpoints failed:", err)
}
```

### 6. Iterator Streaming (`Each`, `Each2`)

```go
items := slices.Values([]string{"order-1", "order-2", "order-3"})

stream := flow.Each(items, func(ctx context.Context, item string) error {
	return processOrder(ctx, item)
})

if err := stream(ctx); err != nil {
	log.Println("Stream interrupted:", err)
}
```

### 7. Idempotent Execution (`Once`)

```go
// Ensure expensive initialization or connection warmup runs only once
initDB := flow.Once(func(ctx context.Context) error {
	return connectDatabase(ctx)
})

stepA := flow.Seq(initDB, queryUsers)
stepB := flow.Seq(initDB, queryOrders)
```

### 8. Directed Acyclic Graph (`DAG`)

```go
userNode := flow.Node("fetch-user", fetchUserStep)
cartNode := flow.Node("fetch-cart", fetchCartStep)

paymentNode := flow.Node("process-payment", processPaymentStep).
	After("fetch-user", "fetch-cart")

notifyNode := flow.Node("send-receipt", sendReceiptStep).
	After("process-payment")

// Independent branches ("fetch-user" and "fetch-cart") run in parallel.
// "process-payment" runs only after both complete.
graph := flow.DAG(userNode, cartNode, paymentNode, notifyNode)
if err := graph(ctx); err != nil {
	log.Println("DAG execution failed:", err)
}
```

## License

MIT License