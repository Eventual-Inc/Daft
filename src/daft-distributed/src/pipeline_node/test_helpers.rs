//! Shared test helpers for distributed pipeline node integration tests.
//!
//! Provides data builders, pipeline construction utilities, and an end-to-end
//! test harness that drives a `DistributedPipelineNode` through the real
//! scheduler → worker → `NativeExecutor` path, collecting aggregated stats.

use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_metrics::{Meter, QueryID, StatSnapshot};
use common_partitioning::PartitionRef;
use common_runtime::JoinSet;
use daft_dsl::expr::{BoundColumn, Expr, bound_expr::BoundExpr};
use daft_local_plan::ExecutionStats;
use daft_logical_plan::InMemoryInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::{
    dtype::DataType,
    field::Field,
    schema::{Schema, SchemaRef},
};
use futures::StreamExt;

use super::{DistributedPipelineNode, in_memory_source::InMemorySourceNode};
use crate::{
    plan::{DistributedPhysicalPlanCollector, PlanConfig, PlanExecutionContext, RunningPlan},

    scheduling::{local_worker::LocalSwordfishWorkerManager, scheduler::spawn_scheduler_actor},
    statistics::StatisticsManager,
};

/// Standard test schema: one Int64 column named "x".
pub fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("x", DataType::Int64)]))
}

/// Create a MicroPartition with a single "x" column of Int64 values.
pub fn make_partition(values: &[i64]) -> Arc<MicroPartition> {
    use daft_core::series::Series;
    let field: daft_schema::field::FieldRef = Field::new("x", DataType::Int64).into();
    let arrow_array: arrow::array::ArrayRef =
        Arc::new(arrow::array::Int64Array::from(values.to_vec()));
    let series = Series::from_arrow(field, arrow_array).unwrap();
    let rb = RecordBatch::new_with_size(test_schema(), vec![series], values.len()).unwrap();
    Arc::new(MicroPartition::new_loaded(
        test_schema(),
        Arc::new(vec![rb]),
        None,
    ))
}

/// Standard test PlanConfig.
pub fn test_plan_config() -> PlanConfig {
    PlanConfig::new(
        0,
        QueryID::from("test-query"),
        Arc::new(DaftExecutionConfig::default()),
    )
}

/// BoundExpr for column "x" at index 0.
pub fn bound_col_x() -> BoundExpr {
    let field = Field::new("x", DataType::Int64);
    #[allow(deprecated)]
    let col = BoundColumn {
        index: 0,
        field: field.into(),
    };
    let expr = Arc::new(Expr::Column(daft_dsl::expr::Column::Bound(col)));
    BoundExpr::new_unchecked(expr)
}

/// BoundExpr for the predicate `x > 2`.
pub fn predicate_x_gt_2() -> BoundExpr {
    let col_expr: daft_dsl::ExprRef = {
        let field = Field::new("x", DataType::Int64);
        #[allow(deprecated)]
        let col = BoundColumn {
            index: 0,
            field: field.into(),
        };
        Arc::new(Expr::Column(daft_dsl::expr::Column::Bound(col)))
    };
    let lit_expr = daft_dsl::lit(2i64);
    let gt_expr = col_expr.gt(lit_expr);
    BoundExpr::new_unchecked(gt_expr)
}

/// Build an InMemorySource node from partitions, returning the node and the
/// plan config used (so callers can chain additional operators).
pub fn build_in_memory_source(
    node_id: u32,
    partitions: Vec<Arc<MicroPartition>>,
    meter: &Meter,
) -> (DistributedPipelineNode, PlanConfig) {
    let plan_config = test_plan_config();
    let schema = test_schema();
    let cache_key = "test-data".to_string();

    let total_rows: usize = partitions.iter().map(|p| p.len()).sum();
    let total_bytes: usize = partitions.iter().map(|p| p.size_bytes()).sum();
    let num_partitions = partitions.len();

    let partition_refs: Vec<PartitionRef> =
        partitions.into_iter().map(|p| p as PartitionRef).collect();

    let mut psets: HashMap<String, Vec<PartitionRef>> = HashMap::new();
    psets.insert(cache_key.clone(), partition_refs);

    let info = InMemoryInfo {
        source_schema: schema,
        cache_key,
        cache_entry: None,
        num_partitions,
        size_bytes: total_bytes,
        num_rows: total_rows,
        clustering_spec: None,
        source_stage_id: None,
    };

    let source_node = InMemorySourceNode::new(node_id, &plan_config, info, Arc::new(psets));
    let node = DistributedPipelineNode::new(Arc::new(source_node), meter);
    (node, plan_config)
}

/// Drive a `DistributedPipelineNode` to completion via the same
/// scheduler → worker → `NativeExecutor` path Ray Flotilla uses,
/// and return the aggregated per-node stats.
pub async fn run_pipeline_and_get_stats(
    pipeline: &DistributedPipelineNode,
    meter: &Meter,
) -> DaftResult<Vec<(Arc<common_metrics::ops::NodeInfo>, StatSnapshot)>> {
    let worker_manager = Arc::new(LocalSwordfishWorkerManager::single_worker());
    run_pipeline_with_manager(pipeline, meter, worker_manager)
        .await
        .map(|stats| stats.nodes)
}

/// Same as [`run_pipeline_and_get_stats`] but accepts a custom worker manager
/// (e.g. for multi-worker tests).
pub async fn run_pipeline_with_manager(
    pipeline: &DistributedPipelineNode,
    meter: &Meter,
    worker_manager: Arc<LocalSwordfishWorkerManager>,
) -> DaftResult<ExecutionStats> {
    let stats_manager = StatisticsManager::from_pipeline_node(pipeline, vec![], meter)?;

    let mut scheduler_joinset = JoinSet::new();
    let scheduler_handle = spawn_scheduler_actor(
        worker_manager,
        &mut scheduler_joinset,
        stats_manager.clone(),
    );

    let mut plan_context = PlanExecutionContext::new(
        0,
        scheduler_handle.clone(),
        DistributedPhysicalPlanCollector::new(),
    );
    let task_stream = pipeline.clone().produce_tasks(&mut plan_context);
    let running_plan = RunningPlan::new(task_stream, plan_context);

    let mut materialized = running_plan.materialize(scheduler_handle.clone());
    while let Some(result) = materialized.next().await {
        let _ = result?;
    }

    drop(scheduler_handle);
    scheduler_joinset.abort_all();

    Ok(stats_manager.export_metrics())
}
