# Conditional Node Execution & DAG Observability

Demonstrates how to conditionally skip DAG nodes at runtime and collect detailed per-node execution telemetry without external dependencies.

## Key Features

1. **Conditional Skipping**:

   ```go
   flow.Node("vip_discount", applyVIPDiscount).
       After("validate").
       When(func(ctx *orderContext) bool { return ctx.isVIP })
   ```

   When `isVIP == false`, the node is skipped (`NodeStatusSkipped`) and downstream nodes (`payment`) continue running without deadlock.

2. **Inline Fluent Guards**:

   ```go
   flow.Node("validate", validateOrder).WithTimeout(2 * time.Second)
   flow.Node("payment", chargeCard).WithRetry(2, 50 * time.Millisecond)
   ```

3. **Execution Report & Telemetry**:

   ```go
   report, err := plan.ExecWithReport(ctx)
   for _, node := range report.Nodes {
       fmt.Println(node.Name, node.Status, node.Duration)
   }
   ```

## Running the Example

```bash
cd examples/dag/6_conditional_and_report
go run main.go
```
