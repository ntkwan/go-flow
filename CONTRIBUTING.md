# Contributing to go-flow

Thank you for your interest in contributing to `go-flow`!

`go-flow` is a zero-dependency, type-safe workflow and pipeline orchestration library for Go. To keep the codebase clean, reliable, and high-performance, all contributions must adhere to the standards outlined below.

---

## Core Principles

1. **Zero External Dependencies**: The root `go.mod` must remain 100% free of external third-party dependencies.
2. **100.0% Statement Coverage**: All statements in root package code must be covered by tests.
3. **Zero Code Comments in `.go` Files**: Code must be self-documenting through clean naming, concise structure, and explicit types.
4. **Deterministic Concurrency & Leak Safety**: All concurrent operations must be race-free and leak-free (`goleak`-verified).

---

## Development Workflow

### Prerequisites

- Go `1.24` or later
- `golangci-lint` (v2)

### Running Checks Locally

Before opening a pull request, ensure all local validation checks pass:

```bash
# Run all formatters, linters, tests, invariants, and BDD suites
make all
```

Individual make targets available:

- `make fmt`: Format source code with `gofmt` and `goimports`.
- `make vet`: Run standard Go static analyzers (`go vet`).
- `make lint`: Run `golangci-lint` across all packages and examples.
- `make vuln`: Run `govulncheck` vulnerability scanner across all modules.
- `make test-race`: Run tests with Go's race detector enabled.
- `make test-alloc`: Run allocation invariant tests.
- `make test-leak`: Run goroutine leak detection tests.
- `make uat`: Run BDD acceptance tests against feature specifications.
- `make cover`: Run tests and inspect statement coverage.

---

## Commit Message Guidelines

This repository enforces the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```text
<type>: <description>
```

Examples:

- `feat: add parallel map-reduce combinator`
- `fix: resolve race in worker pool shutdown`
- `docs: update DAG example in README`
- `perf: reduce allocations in linear pipeline execution`

Do not include scopes in commit messages.

---

## Pull Request Process

1. Fork the repository and create your feature branch from `main`.
2. Implement your changes along with corresponding unit tests and/or BDD `.feature` scenarios.
3. Verify that `make all` passes with 100.0% test coverage.
4. Open a pull request against `main` with a clear description of the change.
