use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_core::prelude::AsArrow;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SchedulingStrategy, SwordfishTask, SwordfishTaskBuilder, TaskID},
        worker::WorkerId,
    },
    utils::channel::{Receiver, Sender, create_channel},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
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

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        // Register shuffle directories for cleanup
        let shuffle_dirs_to_register: Vec<String> = self
            .shuffle_dirs
            .iter()
            .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, self.shuffle_id))
            .collect();
        plan_context.register_shuffle_dirs(shuffle_dirs_to_register);

        // Phase 1: Wrap child tasks with flight_gather_write
        let self_for_instruction = self.clone();
        let wrapped_input = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::flight_gather_write(
                input,
                self_for_instruction.config.schema.clone(),
                self_for_instruction.shuffle_id,
                self_for_instruction.shuffle_dirs.clone(),
                self_for_instruction.compression.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self_for_instruction.context.node_id as usize)),
            )
        });

        let (merge_tx, merge_rx) = create_channel(256);
        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let task_id_counter_output = task_id_counter.clone();
        let scheduler_handle_output = scheduler_handle.clone();
        let result_tx_output = result_tx.clone();

        // Phase 2: execute_merge — accumulate per worker, create merge tasks
        let self_merge = self.clone();
        let merge_execution = async move {
            self_merge
                .execute_merge(
                    wrapped_input,
                    task_id_counter,
                    merge_tx,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        // Phase 3: execute_output — materialize merge tasks, emit read tasks
        let output_execution = async move {
            self.clone()
                .execute_output(
                    merge_rx,
                    task_id_counter_output,
                    result_tx_output,
                    scheduler_handle_output,
                )
                .await
        };

        plan_context.spawn(merge_execution);
        plan_context.spawn(output_execution);
        TaskBuilderStream::from(result_rx)
    }
}

/// Per-worker accumulation bucket tracking cache entries and total bytes.
struct WorkerBucket {
    /// (cache_id, server_address) pairs for each flight_gather_write output
    cache_entries: Vec<(TaskID, String)>,
    total_bytes: usize,
}

impl WorkerBucket {
    fn new() -> Self {
        Self {
            cache_entries: Vec::new(),
            total_bytes: 0,
        }
    }

    fn push(&mut self, task_id: TaskID, server_address: String, data_bytes: usize) {
        self.cache_entries.push((task_id, server_address));
        self.total_bytes += data_bytes;
    }

    fn into_server_cache_mapping(self) -> HashMap<String, Vec<TaskID>> {
        let mut mapping: HashMap<String, Vec<TaskID>> = HashMap::new();
        for (cache_id, server) in self.cache_entries {
            mapping.entry(server).or_default().push(cache_id);
        }
        mapping
    }
}

/// Extract the total data bytes from a flight_gather_write metadata output.
///
/// The output MicroPartition has schema {rows_per_partition: Int32, bytes_per_partition: Int32}
/// with one row per partition (gather mode uses 1 partition).
fn extract_data_bytes(output: &MaterializedOutput) -> usize {
    let mut total_bytes = 0usize;
    for partition in output.partitions() {
        if let Some(mp) = partition.as_any().downcast_ref::<MicroPartition>() {
            for rb in mp.record_batches() {
                // bytes_per_partition is column index 1
                let bytes_col = rb.get_column(1);
                if let Ok(i32_arr) = bytes_col.i32()
                    && let Ok(arrow_arr) = i32_arr.as_arrow()
                {
                    total_bytes += arrow_arr
                        .values()
                        .iter()
                        .map(|v| *v as usize)
                        .sum::<usize>();
                }
            }
        }
    }
    total_bytes
}

impl PreShuffleMergeFlightNode {
    /// Phase 2: Materialize wrapped child tasks (flight_gather_write), accumulate per worker,
    /// create merge tasks when threshold exceeded, pass small buckets through directly.
    async fn execute_merge(
        self: Arc<Self>,
        input_stream: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        merge_tx: Sender<SwordfishTaskBuilder>,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let mut materialized_stream =
            input_stream.materialize(scheduler_handle, self.context.query_idx, task_id_counter);

        let mut worker_buckets: HashMap<WorkerId, WorkerBucket> = HashMap::new();

        while let Some(output) = materialized_stream.try_next().await? {
            let worker_id = output.worker_id().clone();
            let task_id = output.task_id();
            let server_address = output.ip_address().clone();
            let data_bytes = extract_data_bytes(&output);

            let bucket = worker_buckets
                .entry(worker_id.clone())
                .or_insert_with(WorkerBucket::new);
            bucket.push(task_id, server_address, data_bytes);

            if bucket.total_bytes >= self.pre_shuffle_merge_threshold
                && let Some(bucket) = worker_buckets.remove(&worker_id)
            {
                let server_cache_mapping = bucket.into_server_cache_mapping();

                // Create merge task: flight_gather_write(flight_shuffle_read_with_cache_ids(...))
                let read_plan = LocalPhysicalPlan::flight_shuffle_read_with_cache_ids(
                    self.shuffle_id,
                    0, // partition_idx=0 for gather mode
                    server_cache_mapping,
                    self.config.schema.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(self.context.node_id as usize)),
                );
                let merge_plan = LocalPhysicalPlan::flight_gather_write(
                    read_plan,
                    self.config.schema.clone(),
                    self.shuffle_id,
                    self.shuffle_dirs.clone(),
                    self.compression.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(self.context.node_id as usize)),
                );
                let builder = SwordfishTaskBuilder::new(merge_plan, self.as_ref()).with_strategy(
                    Some(SchedulingStrategy::WorkerAffinity {
                        worker_id,
                        soft: false,
                    }),
                );

                if merge_tx.send(builder).await.is_err() {
                    break;
                }
            }
        }

        // Flush remaining small buckets directly as flight_shuffle_read_with_cache_ids tasks
        for (_worker_id, bucket) in worker_buckets {
            if !bucket.cache_entries.is_empty() {
                let server_cache_mapping = bucket.into_server_cache_mapping();
                let read_plan = LocalPhysicalPlan::flight_shuffle_read_with_cache_ids(
                    self.shuffle_id,
                    0, // partition_idx=0 for gather mode
                    server_cache_mapping,
                    self.config.schema.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(self.context.node_id as usize)),
                );
                let builder = SwordfishTaskBuilder::new(read_plan, self.as_ref());
                if result_tx.send(builder).await.is_err() {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Phase 3: Materialize merge tasks, emit flight_shuffle_read_with_cache_ids for each merged cache.
    async fn execute_output(
        self: Arc<Self>,
        merge_rx: Receiver<SwordfishTaskBuilder>,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let merge_stream = TaskBuilderStream::from(merge_rx);
        let mut materialized_merges =
            merge_stream.materialize(scheduler_handle, self.context.query_idx, task_id_counter);

        while let Some(output) = materialized_merges.try_next().await? {
            let server_address = output.ip_address().clone();
            let cache_id = output.task_id();

            let mut server_cache_mapping: HashMap<String, Vec<TaskID>> = HashMap::new();
            server_cache_mapping
                .entry(server_address)
                .or_default()
                .push(cache_id);

            let read_plan = LocalPhysicalPlan::flight_shuffle_read_with_cache_ids(
                self.shuffle_id,
                0, // partition_idx=0 for gather mode
                server_cache_mapping,
                self.config.schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.context.node_id as usize)),
            );
            let builder = SwordfishTaskBuilder::new(read_plan, self.as_ref());
            if result_tx.send(builder).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}
