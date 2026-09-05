# Changelog

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
