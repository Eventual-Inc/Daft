# CLAUDE.md - daft-distributed

This file provides guidance for working with the `daft-distributed` crate, which implements Daft's distributed query execution engine.

## Crate Overview

`daft-distributed` is a core component of Daft's execution engine that handles:
- **Pipeline-based execution**: Converts logical plans into pipeline nodes for distributed execution
- **Task scheduling**: Manages task creation, scheduling, and execution across workers
- **Stage management**: Breaks down complex queries into executable stages
- **Data materialization**: Handles intermediate result storage and data movement

## Architecture

### Key Components

#### Pipeline Nodes (`pipeline_node/`)
Pipeline nodes represent executable units of work that can be distributed:
- **Core trait**: `DistributedPipelineNode` - defines the interface for all pipeline nodes
- **Node types**: Each file represents a different operation (filter, project, join, etc.)
- **Task generation**: Each node produces `SubmittableTask<SwordfishTask>` instances
- **Materialization**: Nodes produce `MaterializedOutput` containing partition data and metadata

#### Scheduling (`scheduling/`)
The scheduling system coordinates task execution:
- **Dispatcher**: Routes tasks to appropriate workers
- **Scheduler**: Manages task queues and execution strategies
- **Workers**: Execute tasks and return results
- **Task types**: `SwordfishTask` is the main task type with execution context

#### Stage Management (`stage/`)
Stages represent execution boundaries where data must be materialized:
- **Stage types**: MapPipeline, HashJoin, HashAggregate, Exchange, Broadcast
- **Stage execution**: Converts stages into running pipeline nodes
- **Stage planning**: `StagePlanBuilder` converts logical plans to stage plans

#### Plan Management (`plan/`)
- **DistributedPhysicalPlan**: Top-level execution plan containing stages
- **PlanRunner**: Orchestrates plan execution across stages
- **Result streaming**: Provides streaming results from plan execution

## Key Data Structures

### MaterializedOutput
Contains the output of completed pipeline nodes:
```rust
pub struct MaterializedOutput {
    partition: Vec<PartitionRef>,  // Actual data partitions
    worker_id: WorkerId,           // Worker that produced the data
}
```

### SwordfishTask
The primary task type for distributed execution:
- Contains a `LocalPhysicalPlan` for execution
- Includes scheduling strategy and partition sets
- Carries execution context and metadata

### SubmittableTaskStream
A stream of tasks that can be submitted for execution:
- Can be materialized into result streams
- Supports pipeline instruction chaining
- Integrates with the scheduler for task execution

## Development Patterns

### Adding New Pipeline Nodes
1. **Create node file**: Add new file in `pipeline_node/` (e.g., `my_operation.rs`)
2. **Implement trait**: Implement `DistributedPipelineNode` trait
3. **Task production**: Implement `produce_tasks()` to generate executable tasks
4. **Display support**: Implement `TreeDisplay` for visualization
5. **Register node**: Add to `translate.rs` for logical plan conversion

### Working with Tasks
- **Task context**: Use `TaskContext` to carry execution metadata
- **Scheduling strategy**: Choose between `Spread`, `Locality`, etc.
- **Partition sets**: Map node IDs to their input partition references
- **Result handling**: Tasks produce `MaterializedOutput` for downstream consumption

### Stage Boundaries
Stages are created at points where:
- Data needs to be shuffled/exchanged between workers
- Aggregations require regrouping of data
- Joins require data co-location
- Broadcasting is needed for small datasets

### Error Handling
- Use `DaftResult<T>` for all fallible operations
- Pipeline nodes should handle partial failures gracefully
- Tasks can fail individually without affecting the entire stage

## Testing Patterns

### Unit Testing
- Test individual pipeline nodes in isolation
- Mock `StageExecutionContext` for testing task generation
- Verify task properties (scheduling strategy, partition sets, etc.)

### Integration Testing
- Test complete stage execution in `scheduling/tests.rs`
- Verify end-to-end plan execution
- Test error propagation and recovery

## Common Development Tasks

### Adding a New Operation
1. **Pipeline node**: Create in `pipeline_node/my_op.rs`
2. **Translation**: Add case in `translate::logical_plan_to_pipeline_node`
3. **Stage support**: Update `StageType` if needed for new execution pattern
4. **Testing**: Add unit tests for the new operation

### Modifying Scheduling
- **Task properties**: Modify `SwordfishTask` for new execution requirements
- **Scheduling strategy**: Add new strategies in `scheduling/task.rs`
- **Worker coordination**: Update worker logic in `scheduling/worker.rs`

### Performance Optimization
- **Task granularity**: Balance between task overhead and parallelism
- **Data locality**: Use worker placement hints in `MaterializedOutput`
- **Memory management**: Optimize partition reference handling
- **Streaming**: Implement streaming execution where possible

## Key Dependencies
- **Tokio**: Async runtime for task execution
- **Futures**: Stream processing and async coordination
- **Serde**: Task and plan serialization for distribution
- **daft-local-plan**: Local execution plans within tasks
- **daft-logical-plan**: Source logical plans for translation

## Integration Points
- **daft-local-execution**: Executes `LocalPhysicalPlan` within tasks
- **daft-physical-plan**: Source of physical plan translations
- **Python bindings**: Exposed through `python/` module for Ray integration
