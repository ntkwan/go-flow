# Changelog

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
