use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

use common_error::DaftResult;
use common_metrics::{
    Counter, DURATION_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, StatSnapshot, UNIT_MICROSECONDS, UNIT_ROWS,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::PreShuffleMergeFlightSnapshot,
};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;
use opentelemetry::{KeyValue, metrics::Meter};

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, PipelineNodeContext,
        PipelineNodeImpl, TaskBuilderStream, metrics::key_values_from_context,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SchedulingStrategy, SwordfishTask, SwordfishTaskBuilder, TaskID},
        worker::WorkerId,
    },
    statistics::{RuntimeStats, stats::RuntimeStatsRef},
    utils::channel::{Sender, create_channel},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}
pub struct PreShuffleMergeFlightStats {
    duration_us: Counter,
    write_duration_us: Counter,
    read_duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl PreShuffleMergeFlightStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            write_duration_us: Counter::new(
                meter,
                "write_duration_us",
                None,
                Some(UNIT_MICROSECONDS.into()),
            ),
            read_duration_us: Counter::new(
                meter,
                "read_duration_us",
                None,
                Some(UNIT_MICROSECONDS.into()),
            ),
            duration_us: Counter::new(meter, DURATION_KEY, None, Some(UNIT_MICROSECONDS.into())),
            rows_in: Counter::new(meter, ROWS_IN_KEY, None, Some(UNIT_ROWS.into())),
            rows_out: Counter::new(meter, ROWS_OUT_KEY, None, Some(UNIT_ROWS.into())),
            node_kv,
        }
    }
}

impl RuntimeStats for PreShuffleMergeFlightStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        match snapshot {
            StatSnapshot::Source(snapshot) => {
                self.duration_us
                    .add(snapshot.cpu_us, self.node_kv.as_slice());
                self.read_duration_us
                    .add(snapshot.cpu_us, self.node_kv.as_slice());
                self.rows_out
                    .add(snapshot.rows_out, self.node_kv.as_slice());
            }
            StatSnapshot::Default(snapshot) => {
                self.duration_us
                    .add(snapshot.cpu_us, self.node_kv.as_slice());
                self.write_duration_us
                    .add(snapshot.cpu_us, self.node_kv.as_slice());
                self.rows_in.add(snapshot.rows_in, self.node_kv.as_slice());
            }
            _ => panic!("Unexpected snapshot type: {:?}", snapshot),
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        let duration_us = self.duration_us.load(Ordering::SeqCst);
        let write_duration_us = self.write_duration_us.load(Ordering::SeqCst);
        let read_duration_us = self.read_duration_us.load(Ordering::SeqCst);
        let rows_in = self.rows_in.load(Ordering::SeqCst);
        let rows_out = self.rows_out.load(Ordering::SeqCst);
        StatSnapshot::PreShuffleMergeFlight(PreShuffleMergeFlightSnapshot {
            duration_us,
            write_duration_us,
            read_duration_us,
            rows_in,
            rows_out,
        })
    }
}

pub(crate) struct PreShuffleMergeFlightNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pre_shuffle_merge_threshold: usize,
    shuffle_id: u64,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    child: DistributedPipelineNode,
}

impl PreShuffleMergeFlightNode {
    const NODE_NAME: NodeName = "PreShuffleMergeFlight";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        pre_shuffle_merge_threshold: usize,
        schema: SchemaRef,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::Repartition,
            NodeCategory::BlockingSink,
        );
        let shuffle_id = make_shuffle_id(&context);
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );

        Self {
            config,
            context,
            pre_shuffle_merge_threshold,
            shuffle_id,
            shuffle_dirs,
            compression,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for PreShuffleMergeFlightNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        vec![
            format!("Pre-Shuffle Merge Flight"),
            format!("Threshold: {}", self.pre_shuffle_merge_threshold),
        ]
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(PreShuffleMergeFlightStats::new(meter, &self.context))
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let shuffle_dirs_to_register: Vec<String> = self
            .shuffle_dirs
            .iter()
            .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, self.shuffle_id))
            .collect();
        plan_context.register_shuffle_dirs(shuffle_dirs_to_register);

        let self_clone = self.clone();
        let write_fn = move |input: LocalPhysicalPlanRef| {
            LocalPhysicalPlan::flight_gather_write(
                input,
                self_clone.config.schema.clone(),
                self_clone.shuffle_id,
                self_clone.shuffle_dirs.clone(),
                self_clone.compression.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self_clone.context.node_id as usize)),
            )
        };
        let write_node = input_node.pipeline_instruction(self.clone(), write_fn);

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let merge_execution =
            self.execute_merge(write_node, task_id_counter, result_tx, scheduler_handle);

        plan_context.spawn(merge_execution);
        TaskBuilderStream::from(result_rx)
    }
}

impl PreShuffleMergeFlightNode {
    async fn execute_merge(
        self: Arc<Self>,
        input_stream: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        write_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let mut materialized_stream =
            input_stream.materialize(scheduler_handle, self.context.query_idx, task_id_counter);

        let mut server_cache_mapping: HashMap<(String, WorkerId), Vec<TaskID>> = HashMap::new();

        while let Some(output) = materialized_stream.try_next().await? {
            let server_address = output.ip_address().clone();
            let worker_id = output.worker_id().clone();
            server_cache_mapping
                .entry((server_address.clone(), worker_id.clone()))
                .or_default()
                .push(output.task_id());

            if server_cache_mapping
                .entry((server_address.clone(), worker_id.clone()))
                .or_default()
                .len()
                >= self.pre_shuffle_merge_threshold
            {
                let single_server_cache_mapping = server_cache_mapping
                    .remove(&(server_address.clone(), worker_id.clone()))
                    .unwrap();
                let mut x = HashMap::new();
                x.insert(server_address.clone(), single_server_cache_mapping);

                let flight_shuffle_read_plan =
                    LocalPhysicalPlan::flight_shuffle_read_with_cache_ids(
                        self.shuffle_id,
                        0,
                        x,
                        self.config.schema.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(self.context.node_id as usize)),
                    );

                let task = SwordfishTaskBuilder::new(flight_shuffle_read_plan, self.as_ref())
                    .with_strategy(Some(SchedulingStrategy::WorkerAffinity {
                        worker_id,
                        soft: false,
                    }));

                if write_tx.send(task).await.is_err() {
                    break;
                }
            }
        }

        for ((ip_address, worker_id), cache_ids) in server_cache_mapping {
            if !cache_ids.is_empty() {
                let mut x = HashMap::new();
                x.insert(ip_address, cache_ids);

                let flight_shuffle_read_plan =
                    LocalPhysicalPlan::flight_shuffle_read_with_cache_ids(
                        self.shuffle_id,
                        0,
                        x,
                        self.config.schema.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(self.context.node_id as usize)),
                    );

                let task = SwordfishTaskBuilder::new(flight_shuffle_read_plan, self.as_ref())
                    .with_strategy(Some(SchedulingStrategy::WorkerAffinity {
                        worker_id,
                        soft: false,
                    }));

                if write_tx.send(task).await.is_err() {
                    break;
                }
            }
        }

        Ok(())
    }
}
