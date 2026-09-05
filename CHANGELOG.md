# Changelog

## [0.6.0](https://github.com/ntkwan/go-flow/compare/v0.5.0...v0.6.0) (2026-09-05)


### Features

* add contributing security guide editor config and govulncheck ([2819781](https://github.com/ntkwan/go-flow/commit/2819781901e72eef97c9881f1be65c9bb2a63664))
* add DAG conditional execution, fluent guards, and telemetry reporting ([9c40ad9](https://github.com/ntkwan/go-flow/commit/9c40ad9825797d7b4e9cb51adfe808cc766d7392))


### Bug Fixes

* ensure deterministic 100% statement coverage in DAG cancellation tests ([8bacf3d](https://github.com/ntkwan/go-flow/commit/8bacf3d426f01e058e6eb7a649d6f68fde248b11))
* resolve markdownlint MD031 and race in DAG test cancellation ([9179bf2](https://github.com/ntkwan/go-flow/commit/9179bf2b891dc5af6836902782d0e4d19c1fa617))

## [0.5.0](https://github.com/ntkwan/go-flow/compare/v0.4.0...v0.5.0) (2026-09-05)


### Features

* add audit-log node to order checkout workflow and sync mermaid diagram ([58b8906](https://github.com/ntkwan/go-flow/commit/58b8906fe9e225bace26ce3c2408a3805b9a6f7f))
* add sentinel errors, cycle path tracing, and plan validation ([fd68bd7](https://github.com/ntkwan/go-flow/commit/fd68bd70c8879e30f24dd48b1c20da3c167d365a))


### Performance Improvements

* reduce dag allocations and add benchmark regression suite ([5677dcc](https://github.com/ntkwan/go-flow/commit/5677dcc0c17e0fa7fc49e0d20022f93824205bee))

## [0.4.0](https://github.com/ntkwan/go-flow/compare/v0.3.0...v0.4.0) (2026-09-05)

### Features

* add dag mermaid export and automated diagram sync ([73a8cc8](https://github.com/ntkwan/go-flow/commit/73a8cc856f7c22ec06fc3fd47ee7c0aef489471d))

## [0.3.0](https://github.com/ntkwan/go-flow/compare/v0.2.0...v0.3.0) (2026-09-05)

### Features

* add advanced concurrency controls ([ce930bb](https://github.com/ntkwan/go-flow/commit/ce930bbdb30a0d49e3389f8c39070e11c1f266bf))
* add branch and pipe combinators ([8857bc8](https://github.com/ntkwan/go-flow/commit/8857bc86eb72c70014aaefdc6bffbeb0487c89c5))
* add comparative examples with and without library ([0c6eae0](https://github.com/ntkwan/go-flow/commit/0c6eae0667f7dc377df0f45d12a3bbb4574ca4d2))
* add core step orchestration and receiver methods ([8982522](https://github.com/ntkwan/go-flow/commit/8982522f709c5d91c177205a376e19e6a67b27fd))
* add dag engine ([fce4286](https://github.com/ntkwan/go-flow/commit/fce42866919efaeb8257b13679a5b75ec25cb32b))
* add pure function edge syntax for dag orchestration ([88b562d](https://github.com/ntkwan/go-flow/commit/88b562d3d08d6e8e3599d871af923b75fef8d1d9))
* add step wrap middleware chunk iterators and dagn execution ([3998b2e](https://github.com/ntkwan/go-flow/commit/3998b2e404bd904fb45f106b3371e1fc9cab177b))

### Bug Fixes

* resolve ci commit lint and static analysis jobs ([15ff0a1](https://github.com/ntkwan/go-flow/commit/15ff0a1dac759734c49ef0d0c66b8ad3cb235699))

## 0.2.0 (2026-09-05)

### Features

* add conditional branching combinator `flow.Branch` and `(s Step[T]) Branch(...)`
* add typed functional value streaming and composition (`flow.Pipe`, `flow.PipeSeq`, `flow.PipeSeq2`, `flow.Pipe2`, `flow.Pipe3`)

## 0.1.0 (2026-09-05)

### Features

* initial release of go-flow workflow orchestration engine
* generic `Step[T]` supporting custom context types
* sequential execution via `Seq` and fluent `.Then()`
* concurrent execution via `Go` and bounded `GoN`
* competitive race execution via `Race`
* directed acyclic graph (DAG) orchestration via `DAG`, `DAGN`, `DAGEdges`, and `From(...).To(...)`
* iterator streaming via `Each` (`iter.Seq`), `Each2` (`iter.Seq2`), `Chunk`, and `Chunk2`
* flow combinators: `Timeout`, `Retry`, `Fallback`, `Catch`, `Recover`, `When`, `Unless`, `Once`, and `Wrap`
* composable middleware pipeline via `Middleware[T]`
