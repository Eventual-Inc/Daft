use std::{
    borrow::Cow,
    sync::{Arc, atomic::Ordering},
};

use common_error::DaftResult;
use common_metrics::{
    Counter, JOIN_BUILD_BYTES_INSERTED_KEY, JOIN_BUILD_ROWS_INSERTED_KEY, JOIN_PROBE_BYTES_IN_KEY,
    JOIN_PROBE_BYTES_OUT_KEY, JOIN_PROBE_ROWS_IN_KEY, JOIN_PROBE_ROWS_OUT_KEY, Meter, StatSnapshot,
    UNIT_BYTES, UNIT_ROWS,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::{JoinSnapshot, StatSnapshotImpl as _},
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{JoinType, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use opentelemetry::KeyValue;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream, metrics::key_values_from_context,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
    utils::channel::{Sender, create_channel},
};

pub struct BroadcastJoinStats {
    duration_us: Counter,
    build_rows_inserted: Counter,
    probe_rows_in: Counter,
    probe_rows_out: Counter,
    build_bytes_inserted: Counter,
    probe_bytes_in: Counter,
    probe_bytes_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl BroadcastJoinStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            duration_us: meter.duration_us_metric(),
            build_rows_inserted: meter.u64_counter_with_desc_and_unit(
                JOIN_BUILD_ROWS_INSERTED_KEY,
                None,
                Some(Cow::Borrowed(UNIT_ROWS)),
            ),
            probe_rows_in: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_ROWS_IN_KEY,
                None,
                Some(Cow::Borrowed(UNIT_ROWS)),
            ),
            probe_rows_out: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_ROWS_OUT_KEY,
                None,
                Some(Cow::Borrowed(UNIT_ROWS)),
            ),
            build_bytes_inserted: meter.u64_counter_with_desc_and_unit(
                JOIN_BUILD_BYTES_INSERTED_KEY,
                None,
                Some(Cow::Borrowed(UNIT_BYTES)),
            ),
            probe_bytes_in: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_BYTES_IN_KEY,
                None,
                Some(Cow::Borrowed(UNIT_BYTES)),
            ),
            probe_bytes_out: meter.u64_counter_with_desc_and_unit(
                JOIN_PROBE_BYTES_OUT_KEY,
                None,
                Some(Cow::Borrowed(UNIT_BYTES)),
            ),
            node_kv: key_values_from_context(context),
        }
    }

    pub fn set_build_rows_inserted(&self, rows: u64) {
        self.build_rows_inserted.add(rows, self.node_kv.as_slice());
    }

    pub fn set_build_bytes_inserted(&self, bytes: u64) {
        self.build_bytes_inserted
            .add(bytes, self.node_kv.as_slice());
    }
}

impl RuntimeStats for BroadcastJoinStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.duration_us
            .add(snapshot.duration_us(), self.node_kv.as_slice());
        let StatSnapshot::Join(snapshot) = snapshot else {
            return;
        };

        self.probe_rows_in
            .add(snapshot.probe_rows_in, self.node_kv.as_slice());
        self.probe_rows_out
            .add(snapshot.probe_rows_out, self.node_kv.as_slice());
        self.probe_bytes_in
            .add(snapshot.probe_bytes_in, self.node_kv.as_slice());
        self.probe_bytes_out
            .add(snapshot.probe_bytes_out, self.node_kv.as_slice());
    }

    fn export_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Join(JoinSnapshot {
            cpu_us: self.duration_us.load(Ordering::SeqCst),
            build_rows_inserted: self.build_rows_inserted.load(Ordering::SeqCst),
            probe_rows_in: self.probe_rows_in.load(Ordering::SeqCst),
            probe_rows_out: self.probe_rows_out.load(Ordering::SeqCst),
            build_bytes_inserted: self.build_bytes_inserted.load(Ordering::SeqCst),
            probe_bytes_in: self.probe_bytes_in.load(Ordering::SeqCst),
            probe_bytes_out: self.probe_bytes_out.load(Ordering::SeqCst),
        })
    }
}

pub(crate) struct BroadcastJoinNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,

    // Join properties
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    null_equals_nulls: Option<Vec<bool>>,
    join_type: JoinType,
    is_swapped: bool,

    broadcaster: DistributedPipelineNode,
    broadcaster_schema: SchemaRef,
    receiver: DistributedPipelineNode,
    runtime_stats: Arc<BroadcastJoinStats>,
}

impl BroadcastJoinNode {
    const NODE_NAME: &'static str = "BroadcastJoin";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        is_swapped: bool,
        broadcaster: DistributedPipelineNode,
        receiver: DistributedPipelineNode,
        output_schema: SchemaRef,
        meter: &Meter,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::BroadcastJoin,
            NodeCategory::BlockingSink,
        );

        // For broadcast joins, we use the receiver's clustering spec since the broadcaster
        // will be gathered to all partitions
        let config = PipelineNodeConfig::new(
            output_schema,
            plan_config.config.clone(),
            receiver.config().clustering_spec.clone(),
        );
        let broadcaster_schema = broadcaster.config().schema.clone();
        let runtime_stats = Arc::new(BroadcastJoinStats::new(meter, &context));

        Self {
            config,
            context,
            left_on,
            right_on,
            null_equals_nulls,
            join_type,
            is_swapped,
            broadcaster,
            broadcaster_schema,
            receiver,
            runtime_stats,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        broadcaster_input: TaskBuilderStream,
        mut receiver_input: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_broadcast_data = broadcaster_input
            .materialize(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter.clone(),
            )
            .try_collect::<Vec<_>>()
            .await?;

        let build_rows = materialized_broadcast_data
            .iter()
            .map(|output| output.num_rows())
            .sum::<usize>();
        let build_bytes = materialized_broadcast_data
            .iter()
            .map(|output| output.size_bytes())
            .sum::<usize>();
        self.runtime_stats
            .set_build_rows_inserted(build_rows as u64);
        self.runtime_stats
            .set_build_bytes_inserted(build_bytes as u64);

        let (materialized_broadcast_data_plan, broadcast_psets) =
            MaterializedOutput::into_in_memory_scan_with_psets(
                materialized_broadcast_data,
                self.broadcaster_schema.clone(),
                self.node_id(),
            );
        while let Some(builder) = receiver_input.next().await {
            // Transform the plan using map_plan to avoid exposing builder internals
            let new_builder = builder
                .map_plan(self.as_ref(), |input_plan| {
                    let (left_plan, right_plan) = if self.is_swapped {
                        (input_plan, materialized_broadcast_data_plan.clone())
                    } else {
                        (materialized_broadcast_data_plan.clone(), input_plan)
                    };

                    // We want to build on the broadcast side, so if swapped, build on the right side
                    let build_on_left = !self.is_swapped;
                    LocalPhysicalPlan::hash_join(
                        left_plan,
                        right_plan,
                        self.left_on.clone(),
                        self.right_on.clone(),
                        Some(build_on_left),
                        self.null_equals_nulls.clone(),
                        self.join_type,
                        self.config.schema.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(self.node_id() as usize)),
                    )
                })
                .with_psets(self.node_id(), broadcast_psets.clone());

            if result_tx.send(new_builder).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

impl PipelineNodeImpl for BroadcastJoinNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.broadcaster.clone(), self.receiver.clone()]
    }

    fn make_runtime_stats(&self, _meter: &Meter) -> RuntimeStatsRef {
        self.runtime_stats.clone()
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec!["BroadcastJoin".to_string()];
        res.push(format!("Type: {}", self.join_type));
        res.push(format!(
            "Left: Join key = {}, Role = {}",
            self.left_on.iter().map(|e| e.to_string()).join(", "),
            if self.is_swapped {
                "Receiver"
            } else {
                "Broadcaster"
            }
        ));
        res.push(format!(
            "Right: Join key = {}, Role = {}",
            self.right_on.iter().map(|e| e.to_string()).join(", "),
            if self.is_swapped {
                "Broadcaster"
            } else {
                "Receiver"
            }
        ));
        if let Some(null_equals_nulls) = &self.null_equals_nulls {
            res.push(format!(
                "Null equals nulls: [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let broadcaster_input = self.broadcaster.clone().produce_tasks(plan_context);
        let receiver_input = self.receiver.clone().produce_tasks(plan_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            broadcaster_input,
            receiver_input,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        );
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }
}
