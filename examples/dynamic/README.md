# Dynamic / Lazy Pipeline Branching

Demonstrates how to evaluate and construct execution steps dynamically at runtime based on context state.

## Key Features

1. **Lazy Step Resolution**:

   ```go
   workflow := flow.Dynamic(func(ctx *userContext) flow.Step[*userContext] {
       switch ctx.Tier {
       case "enterprise":
           return enterpriseTierWorkflow
       case "pro":
           return proTierWorkflow
       default:
           return freeTierWorkflow
       }
   })
   ```

2. **Fluent Method Chaining**:

   ```go
   pipeline := initialStep.Dynamic(func(ctx *userContext) flow.Step[*userContext] {
       return nextStepForUser(ctx)
   })
   ```

## Running the Example

```bash
cd examples/dynamic/with_flow
go run main.go
```
