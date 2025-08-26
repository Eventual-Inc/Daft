# daft-distributed

This crate implements Daft's distributed execution engine using an actor-based architecture for multi-machine query processing.

## Architecture Overview

The distributed execution model works as follows:
1. **Pipeline nodes** create `LocalPhysicalPlan` tasks (not serializable themselves)
2. **Scheduler** receives tasks from pipeline nodes and submits them to workers
3. **Worker manager** coordinates Ray actors implemented in Python (`daft/runners/flotilla.py`)
4. **Ray workers** execute the `LocalPhysicalPlan` tasks using `daft-local-execution`

### Pipeline Nodes (`pipeline_node/`)
Core execution operators that **schedule tasks** rather than execute directly:

- **Data Movement**: `shuffles/`, `gather.rs`, `repartition.rs`
- **Joins**: `join/` (broadcast, hash, translate)
- **Aggregations**: `aggregate.rs`, `distinct.rs`, `window.rs`
- **Transformations**: `project.rs`, `filter.rs`, `explode.rs`, `udf.rs`
- **I/O**: `scan_source.rs`, `sink.rs`, `in_memory_source.rs`
- **Utilities**: `limit.rs`, `sort.rs`, `top_n.rs`, `sample.rs`

Pipeline nodes are **not serializable** - they generate serializable `LocalPhysicalPlan` tasks for worker execution.

### Scheduling (`scheduling/`)
Task orchestration and worker management:

- `dispatcher.rs`: Task distribution logic
- `scheduler/`: Receives tasks from pipeline nodes, submits to workers
- `worker.rs`: Worker node coordination
- `task.rs`: `LocalPhysicalPlan` task definitions

### Python Integration (`python/`)
Ray-based distributed execution via `daft/runners/flotilla.py`:

- `ray/`: Ray worker integration and task execution
- Workers are Ray actors that run `LocalPhysicalPlan` via `daft-local-execution`

### Execution Planning (`plan/`)
- `runner.rs`: Distributed plan execution coordination
- `stage/`: Multi-stage execution planning

## Key Components

### Task Flow
1. Pipeline nodes schedule `LocalPhysicalPlan` tasks to scheduler
2. Scheduler submits tasks to worker manager
3. Worker manager coordinates Ray actors in `daft/runners/flotilla.py`
4. Ray workers execute plans using `daft-local-execution` engine

### Shuffle Exchange (`shuffle_exchange/`)
Handles data redistribution between workers for operations requiring specific partitioning.

### Statistics (`statistics/`)
HTTP-based metrics collection for monitoring distributed execution performance.

### Utilities (`utils/`)
Common distributed execution helpers:
- `channel.rs`: Actor communication primitives
- `runtime.rs`: Async runtime management
- `stream.rs`: Streaming data processing utilities

## Development Notes

- Pipeline nodes generate serializable `LocalPhysicalPlan` tasks, not themselves serializable
- Ray workers in Python execute these plans via `daft-local-execution`
- Scheduler acts as intermediary between pipeline nodes and worker manager
- Uses Ray as the distributed computing backend through Python actor integration
