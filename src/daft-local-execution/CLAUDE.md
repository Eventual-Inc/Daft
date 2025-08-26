# daft-local-execution

This crate implements Daft's single-machine execution engine using a streaming pipeline architecture. It executes `LocalPhysicalPlan` tasks both locally and as distributed tasks submitted by `daft-distributed`.

## Architecture Overview

The local execution engine uses a push-based streaming model with three operator types:

### Sources (`sources/`)
Data input operators that produce streams:
- `scan_task.rs`: File/object scanning with pushdown optimization
- `in_memory.rs`: In-memory data sources
- `empty_scan.rs`: Empty/null data sources

### Intermediate Ops (`intermediate_ops/`)
Streaming transformations that process data row-by-row or batch-by-batch:
- `project.rs`: Column projection and expression evaluation
- `filter.rs`: Row filtering with predicate pushdown
- `explode.rs`: Array/list expansion operations
- `udf.rs`: User-defined function execution
- `unpivot.rs`: Column-to-row transformation
- `sample.rs`: Data sampling operations
- `cross_join.rs`: Cartesian product operations
- `inner_hash_join_probe.rs`: Hash join probe side
- `distributed_actor_pool_project.rs`: Distributed UDF execution
- `into_batches.rs`: Batch size control

### Sinks (`sinks/`)
Terminal operators that consume streams:

**Blocking Sinks** (collect all data before output):
- `aggregate.rs`: Global aggregations
- `grouped_aggregate.rs`: GROUP BY aggregations
- `sort.rs`: Global sorting
- `top_n.rs`: Top-K operations
- `hash_join_build.rs`: Hash join build side
- `cross_join_collect.rs`: Cross join materialization
- `dedup.rs`: Duplicate elimination
- `pivot.rs`: Row-to-column transformation

**Streaming Sinks** (`streaming_sink/`):
- `concat.rs`: Stream concatenation
- `limit.rs`: Result limiting
- `monotonically_increasing_id.rs`: ID generation
- `anti_semi_hash_join_probe.rs`: Anti/semi join probe
- `outer_hash_join_probe.rs`: Left/right/full outer join probe

**Partitioning/Output**:
- `into_partitions.rs`: Data partitioning
- `repartition.rs`: Repartitioning operations
- `write.rs`: File/object writing
- `commit_write.rs`: Transactional write commits

**Window Functions**:
- `window_base.rs`: Common window function logic
- `window_order_by_only.rs`: ROW_NUMBER, RANK, etc.
- `window_partition_only.rs`: Partition-level aggregations
- `window_partition_and_order_by.rs`: Full window functions
- `window_partition_and_dynamic_frame.rs`: Dynamic frame windows

## Core Components

### Execution Runtime
- `run.rs`: Main execution entry point for `LocalPhysicalPlan`
- `pipeline.rs`: Pipeline construction and execution coordination
- `dispatcher.rs`: Task scheduling and worker coordination
- `ops.rs`: Common operator traits and utilities

### Resource Management
- `resource_manager.rs`: Memory and CPU resource tracking
- `buffer.rs`: Streaming buffer management
- `channel.rs`: Inter-operator communication channels

### Monitoring (`runtime_stats/`)
- `values.rs`: Performance metrics collection
- `subscribers/`: Various monitoring backends:
  - `dashboard.rs`: Web dashboard integration
  - `debug.rs`: Debug logging
  - `opentelemetry.rs`: OpenTelemetry integration
  - `progress_bar.rs`: Progress tracking

### State Management
- `state_bridge.rs`: State sharing between operators

## Execution Model

1. **Pipeline Construction**: `LocalPhysicalPlan` → operator pipeline
2. **Streaming Execution**: Push-based data flow through operators
3. **Resource Management**: Automatic memory and CPU resource tracking
4. **Backpressure**: Flow control to prevent memory exhaustion

## Usage Patterns

- **Local Execution**: Direct execution of DataFrame operations
- **Distributed Tasks**: Execution of tasks submitted by `daft-distributed`
- **Ray Workers**: Execution within Python Ray actors (`daft/runners/flotilla.py`)

## Development Notes

- Operators implement streaming interfaces for memory efficiency
- Blocking sinks accumulate data; streaming sinks process incrementally
- Resource management prevents OOM conditions in large datasets
- Pipeline construction optimizes operator ordering and resource usage
