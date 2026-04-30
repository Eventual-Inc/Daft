//! Integration tests for `OperatorStart` / `OperatorEnd` lifecycle event
//! emission on the distributed runner.
//!
//! These tests drive a real `DistributedPipelineNode` through the same
//! scheduler → worker → `NativeExecutor` path the Ray Flotilla runner
//! uses (via `LocalSwordfishWorkerManager::single_worker`), so no Ray
//! cluster is needed. Events are observed via the buffer that
//! `StatisticsManager` populates and that the driver-side Flotilla
//! wrapper drains in production.

use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::Meter;
use daft_micropartition::MicroPartition;
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{
    DistributedPipelineNode, filter::FilterNode,
    test_helpers::{
        CapturedRun, build_in_memory_source, make_partition, predicate_x_gt_2,
        run_pipeline_and_capture_events, test_schema,
    },
};
use crate::{
    plan::{PlanExecutionContext, RunningPlan},
    scheduling::{local_worker::LocalSwordfishWorkerManager, scheduler::spawn_scheduler_actor},
    statistics::{PendingOperatorEvent, PendingOperatorKind, StatisticsManager},
};

fn build_filter_pipeline(
    partitions: Vec<Arc<MicroPartition>>,
    meter: &Meter,
) -> (DistributedPipelineNode, SchemaRef) {
    let (source, plan_config) = build_in_memory_source(0, partitions, meter);
    let schema = test_schema();
    let filter_node = FilterNode::new(1, &plan_config, predicate_x_gt_2(), schema.clone(), source);
    (
        DistributedPipelineNode::new(Arc::new(filter_node), meter),
        schema,
    )
}

fn starts(events: &[PendingOperatorEvent]) -> Vec<&PendingOperatorEvent> {
    events
        .iter()
        .filter(|e| e.kind == PendingOperatorKind::Start)
        .collect()
}

fn ends(events: &[PendingOperatorEvent]) -> Vec<&PendingOperatorEvent> {
    events
        .iter()
        .filter(|e| e.kind == PendingOperatorKind::End)
        .collect()
}

fn position_of(
    events: &[PendingOperatorEvent],
    kind: PendingOperatorKind,
    node_id: u32,
) -> Option<usize> {
    events
        .iter()
        .position(|e| e.kind == kind && e.node_id == node_id)
    // Note: positions reflect buffer insertion order, which matches
    // the order events were dispatched.
}

/// A source with at least one task fires exactly one Start and one End,
/// in that order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_only_pipeline_emits_one_start_then_one_end() -> DaftResult<()> {
    let meter = Meter::test_scope("lifecycle_source_only");
    let (pipeline, _cfg) = build_in_memory_source(0, vec![make_partition(&[1, 2, 3])], &meter);

    let captured = run_pipeline_and_capture_events(&pipeline, &meter).await?;

    let s = starts(&captured.events);
    let e = ends(&captured.events);
    assert_eq!(s.len(), 1, "expected one Start, got {}: {:?}", s.len(), s);
    assert_eq!(e.len(), 1, "expected one End, got {}: {:?}", e.len(), e);
    assert_eq!(s[0].node_id, 0);
    assert_eq!(e[0].node_id, 0);
    assert_eq!(s[0].name.as_ref(), "InMemorySource");

    let start_pos = position_of(&captured.events, PendingOperatorKind::Start, 0).unwrap();
    let end_pos = position_of(&captured.events, PendingOperatorKind::End, 0).unwrap();
    assert!(start_pos < end_pos, "Start should precede End for node 0");

    Ok(())
}

/// A source with zero partitions never starts, so neither Start nor End
/// fires — the lifecycle gate (`started==true` required for End) keeps
/// us from emitting an orphaned End.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_source_emits_no_lifecycle_events() -> DaftResult<()> {
    let meter = Meter::test_scope("lifecycle_empty_source");
    let (pipeline, _cfg) = build_in_memory_source(0, vec![], &meter);

    let captured = run_pipeline_and_capture_events(&pipeline, &meter).await?;

    assert!(
        captured.events.is_empty(),
        "expected no events for empty source, got: {:?}",
        captured.events
    );
    Ok(())
}

/// Source → Filter (a single pass-through pipeline): both nodes run
/// inside the same task, so each gets one Start before any End. Each
/// node has a single Start/End pair and operator names propagate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_filter_pipeline_emits_paired_events() -> DaftResult<()> {
    let meter = Meter::test_scope("lifecycle_source_filter");
    let (pipeline, _schema) = build_filter_pipeline(vec![make_partition(&[1, 2, 3, 4, 5])], &meter);

    let captured = run_pipeline_and_capture_events(&pipeline, &meter).await?;

    let s = starts(&captured.events);
    let e = ends(&captured.events);
    assert_eq!(s.len(), 2, "expected 2 Start events, got: {:?}", s);
    assert_eq!(e.len(), 2, "expected 2 End events, got: {:?}", e);

    let start_ids: Vec<_> = s.iter().map(|x| x.node_id).collect();
    let end_ids: Vec<_> = e.iter().map(|x| x.node_id).collect();
    assert!(start_ids.contains(&0) && start_ids.contains(&1));
    assert!(end_ids.contains(&0) && end_ids.contains(&1));

    let names: std::collections::HashMap<_, _> =
        captured.events.iter().map(|e| (e.node_id, e.name.as_ref())).collect();
    assert_eq!(names.get(&0).copied(), Some("InMemorySource"));
    assert_eq!(names.get(&1).copied(), Some("Filter"));

    for node_id in [0u32, 1] {
        let sp = position_of(&captured.events, PendingOperatorKind::Start, node_id).unwrap();
        let ep = position_of(&captured.events, PendingOperatorKind::End, node_id).unwrap();
        assert!(sp < ep, "Start must precede End for node {node_id}");
    }
    Ok(())
}

/// Multiple input partitions still produce exactly one Start/End pair
/// per node, regardless of how many tasks reference that node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_partition_source_filter_dedupes_events_per_node() -> DaftResult<()> {
    let meter = Meter::test_scope("lifecycle_multi_partition");
    let (pipeline, _schema) = build_filter_pipeline(
        vec![
            make_partition(&[1, 2, 3]),
            make_partition(&[4, 5]),
            make_partition(&[1, 1]),
        ],
        &meter,
    );

    let captured = run_pipeline_and_capture_events(&pipeline, &meter).await?;

    let s = starts(&captured.events);
    let e = ends(&captured.events);
    assert_eq!(s.len(), 2);
    assert_eq!(e.len(), 2);
    Ok(())
}

/// If the result stream is dropped before all tasks complete, the
/// `OnEndStream` `Drop` path still signals `notify_produce_complete`,
/// and the dispatcher's in-flight tasks then complete naturally
/// (emitting `Completed` / `Cancelled` events that drain
/// `pending_tasks`), so `OperatorEnd` eventually fires for every node
/// that fired `OperatorStart`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_result_stream_still_finalizes_operators() -> DaftResult<()> {
    let meter = Meter::test_scope("lifecycle_drop");
    let pipeline = {
        let parts: Vec<Arc<MicroPartition>> = (0..16)
            .map(|i| make_partition(&[i as i64, i as i64 + 1]))
            .collect();
        let (source, plan_config) = build_in_memory_source(0, parts, &meter);
        let filter_node = FilterNode::new(
            1,
            &plan_config,
            predicate_x_gt_2(),
            test_schema(),
            source,
        );
        DistributedPipelineNode::new(Arc::new(filter_node), &meter)
    };

    let captured = run_pipeline_and_capture_first_n(&pipeline, &meter, 1).await?;

    let started: std::collections::HashSet<_> =
        starts(&captured.events).iter().map(|e| e.node_id).collect();
    let ended: std::collections::HashSet<_> =
        ends(&captured.events).iter().map(|e| e.node_id).collect();
    assert!(
        !started.is_empty(),
        "expected at least one Start before drop"
    );
    assert_eq!(
        started, ended,
        "every started node should also End on cancellation; started={started:?} ended={ended:?}",
    );
    Ok(())
}

/// Hard-abort path: scheduler tasks are aborted before they can emit
/// terminal `TaskEvent`s, so `pending_tasks` never naturally drains and
/// the lifecycle gate keeps `OperatorEnd` from firing. Calling
/// `flush_started_operators` (the same hook `PythonPartitionRefStream::finish`
/// uses) synthesizes `OperatorEnd` for every started node so subscribers
/// don't see orphaned Starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_recovers_started_operators_after_hard_abort() -> DaftResult<()> {
    let meter = Meter::test_scope("lifecycle_flush_after_abort");
    let pipeline = {
        let parts: Vec<Arc<MicroPartition>> = (0..32)
            .map(|i| make_partition(&[i as i64, i as i64 + 1]))
            .collect();
        let (source, plan_config) = build_in_memory_source(0, parts, &meter);
        let filter_node = FilterNode::new(
            1,
            &plan_config,
            predicate_x_gt_2(),
            test_schema(),
            source,
        );
        DistributedPipelineNode::new(Arc::new(filter_node), &meter)
    };

    let worker_manager = Arc::new(LocalSwordfishWorkerManager::single_worker());
    let stats_manager = StatisticsManager::from_pipeline_node(
        &pipeline,
        vec![],
        &meter,
        "test-query".into(),
    )?;

    let mut scheduler_joinset = common_runtime::JoinSet::new();
    let scheduler_handle = spawn_scheduler_actor(
        worker_manager,
        &mut scheduler_joinset,
        stats_manager.clone(),
    );
    let mut plan_context =
        PlanExecutionContext::new(0, scheduler_handle.clone(), stats_manager.clone());
    let task_stream = pipeline.clone().produce_tasks(&mut plan_context);
    let running_plan = RunningPlan::new(task_stream, plan_context);

    let mut materialized = running_plan.materialize(scheduler_handle.clone());
    // Take a single result, then forcibly abort everything — simulates
    // an actor crash / KeyboardInterrupt mid-query.
    let _ = materialized.next().await;
    drop(materialized);
    drop(scheduler_handle);
    scheduler_joinset.abort_all();
    while scheduler_joinset.join_next().await.is_some() {}

    let pre_flush = stats_manager.take_pending_operator_events();
    let started_pre: std::collections::HashSet<_> = pre_flush
        .iter()
        .filter(|e| e.kind == PendingOperatorKind::Start)
        .map(|e| e.node_id)
        .collect();
    let ended_pre: std::collections::HashSet<_> = pre_flush
        .iter()
        .filter(|e| e.kind == PendingOperatorKind::End)
        .map(|e| e.node_id)
        .collect();
    assert!(
        !started_pre.is_empty(),
        "expected Start events to have fired before abort"
    );
    assert!(
        ended_pre.len() < started_pre.len(),
        "abort should leave at least one orphan Start; started={started_pre:?} ended={ended_pre:?}",
    );

    // Now flush — the hook PythonPartitionRefStream::finish uses.
    stats_manager.flush_started_operators();
    let post_flush = stats_manager.take_pending_operator_events();

    let synthetic_ends: std::collections::HashSet<_> = post_flush
        .iter()
        .filter(|e| e.kind == PendingOperatorKind::End)
        .map(|e| e.node_id)
        .collect();
    let total_ended: std::collections::HashSet<_> =
        ended_pre.union(&synthetic_ends).copied().collect();
    assert_eq!(
        total_ended, started_pre,
        "after flush, every started node must have an End; started={started_pre:?} \
         ended_pre={ended_pre:?} synthetic_ends={synthetic_ends:?}",
    );

    // Idempotent: a second flush must be a no-op.
    stats_manager.flush_started_operators();
    let second_flush = stats_manager.take_pending_operator_events();
    assert!(
        second_flush.is_empty(),
        "flush_started_operators should be idempotent; got {second_flush:?}",
    );
    Ok(())
}

/// Variant of the run helper that takes only the first `n` results from
/// the materialized stream and then drops it, exercising the cancellation
/// / early-termination path. We deliberately drive `scheduler_joinset` to
/// natural completion (no `abort_all`) so in-flight tasks emit their
/// terminal `TaskEvent`s — that's the same lifecycle the production
/// driver-side `stream_plan` sees when its caller stops iterating.
async fn run_pipeline_and_capture_first_n(
    pipeline: &DistributedPipelineNode,
    meter: &Meter,
    n: usize,
) -> DaftResult<CapturedRun> {
    let worker_manager = Arc::new(LocalSwordfishWorkerManager::single_worker());
    let stats_manager = StatisticsManager::from_pipeline_node(
        pipeline,
        vec![],
        meter,
        "test-query".into(),
    )?;

    let mut scheduler_joinset = common_runtime::JoinSet::new();
    let scheduler_handle = spawn_scheduler_actor(
        worker_manager,
        &mut scheduler_joinset,
        stats_manager.clone(),
    );

    let mut plan_context =
        PlanExecutionContext::new(0, scheduler_handle.clone(), stats_manager.clone());
    let task_stream = pipeline.clone().produce_tasks(&mut plan_context);
    let running_plan = RunningPlan::new(task_stream, plan_context);

    let mut materialized = running_plan.materialize(scheduler_handle.clone());
    let mut taken = 0;
    while taken < n {
        match materialized.next().await {
            Some(result) => {
                let _ = result?;
                taken += 1;
            }
            None => break,
        }
    }
    drop(materialized);
    drop(scheduler_handle);

    while let Some(res) = scheduler_joinset.join_next().await {
        // Surface unexpected join errors but treat task-level errors as
        // expected (they're the cancellation path under test).
        let _ = res;
    }

    let stats = stats_manager.export_metrics();
    let events = stats_manager.take_pending_operator_events();
    Ok(CapturedRun { events, stats })
}
