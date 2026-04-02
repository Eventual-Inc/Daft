use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use common_metrics::{
    Meter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::StatSnapshotImpl as _,
};

pub(crate) const SHUFFLE_WRITE_PHASE: &str = "shuffle-write";
pub(crate) const SHUFFLE_READ_PHASE: &str = "shuffle-read";
use daft_local_plan::{
    FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend,
    ShuffleWriteBackend,
};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        ShufflePartitionRef, TaskBuilderStream, TaskOutput,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    statistics::{
        RuntimeStats,
        stats::{BaseCounters, RuntimeStatsRef, SimpleCounters},
    },
    utils::channel::{Sender, create_channel},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

struct FlightDistributedShuffleConfig {
    shuffle_id: u64,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
}

struct FlightReadSpec {
    shuffle_id: u64,
    server_cache_mapping: HashMap<String, Vec<u32>>,
}

enum DistributedShuffleBackend {
    Flight(FlightDistributedShuffleConfig),
}

pub(crate) struct ShuffleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    backend: DistributedShuffleBackend,
    child: DistributedPipelineNode,
}

impl ShuffleNode {
    const NODE_NAME: &'static str = "FlightShuffle";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Repartition,
            NodeCategory::BlockingSink,
        );
        let shuffle_id = make_shuffle_id(&context);
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        Self {
            config,
            context,
            repartition_spec,
            num_partitions,
            backend: DistributedShuffleBackend::Flight(FlightDistributedShuffleConfig {
                shuffle_id,
                shuffle_dirs,
                compression,
            }),
            child,
        }
    }

    fn register_flight_cleanup(
        &self,
        plan_context: &mut PlanExecutionContext,
        backend: &FlightDistributedShuffleConfig,
    ) {
        let shuffle_dirs_to_register: Vec<String> = backend
            .shuffle_dirs
            .iter()
            .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, backend.shuffle_id))
            .collect();
        plan_context.register_shuffle_dirs(shuffle_dirs_to_register);
    }

    fn build_flight_write_stage(
        self: Arc<Self>,
        input_node: TaskBuilderStream,
        backend: &FlightDistributedShuffleConfig,
    ) -> TaskBuilderStream {
        let partition_by = match &self.repartition_spec {
            RepartitionSpec::Hash(hash_spec) => Some(hash_spec.by.clone()),
            RepartitionSpec::Random(_) => None,
            RepartitionSpec::Range(_) => {
                unreachable!("Range repartition is not supported for flight shuffle")
            }
        };

        let shuffle_id = backend.shuffle_id;
        let shuffle_dirs = backend.shuffle_dirs.clone();
        let compression = backend.compression.clone();
        input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::shuffle_write(
                input,
                partition_by.clone(),
                self.num_partitions,
                self.config.schema.clone(),
                ShuffleWriteBackend::Flight {
                    shuffle_id,
                    shuffle_dirs: shuffle_dirs.clone(),
                    compression: compression.clone(),
                },
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.context.node_id as usize))
                    .with_phase("shuffle-write"),
            )
        })
    }

    fn flight_read_spec_from_outputs(
        &self,
        backend: &FlightDistributedShuffleConfig,
        outputs: Vec<TaskOutput>,
    ) -> DaftResult<FlightReadSpec> {
        let mut server_cache_mapping: HashMap<String, HashSet<u32>> = HashMap::new();

        for output in outputs {
            let TaskOutput::ShuffleWrite(output) = output else {
                return Err(common_error::DaftError::InternalError(
                    "Expected shuffle write task output for Flight shuffle write stage".to_string(),
                ));
            };

            for partition in output.partitions {
                match partition {
                    ShufflePartitionRef::Flight(partition) => {
                        server_cache_mapping
                            .entry(partition.server_address)
                            .or_default()
                            .insert(partition.cache_id);
                    }
                }
            }
        }

        Ok(FlightReadSpec {
            shuffle_id: backend.shuffle_id,
            server_cache_mapping: server_cache_mapping
                .into_iter()
                .map(|(server, cache_ids)| (server, cache_ids.into_iter().collect()))
                .collect(),
        })
    }

    async fn emit_flight_read_tasks(
        &self,
        read_spec: FlightReadSpec,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        for partition_idx in 0..self.num_partitions {
            let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
                self.context.node_id,
                self.config.schema.clone(),
                ShuffleReadBackend::Flight {
                    shuffle_id: read_spec.shuffle_id,
                    server_cache_mapping: read_spec.server_cache_mapping.clone(),
                },
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.context.node_id as usize))
                    .with_phase("shuffle-read"),
            );

            let task = SwordfishTaskBuilder::new(shuffle_read_plan, self, self.node_id())
                .with_flight_shuffle_reads(
                    self.context.node_id,
                    vec![FlightShuffleReadInput { partition_idx }],
                );

            let _ = result_tx.send(task).await;
        }

        Ok(())
    }

    // Async execution to handle flight shuffle write and read operations
    async fn execution_loop(
        self: Arc<Self>,
        local_shuffle_write_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let outputs = local_shuffle_write_node
            .task_outputs(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter,
            )
            .try_collect::<Vec<_>>()
            .await?;

        match &self.backend {
            DistributedShuffleBackend::Flight(backend) => {
                let read_spec = self.flight_read_spec_from_outputs(backend, outputs)?;
                self.emit_flight_read_tasks(read_spec, result_tx).await?;
            }
        }

        Ok(())
    }
}

pub struct ShuffleStats {
    base: BaseCounters,
    node_id: u32,
    write_counters: SimpleCounters,
    read_counters: SimpleCounters,
}

impl ShuffleStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext, node_id: u32) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
            node_id,
            write_counters: SimpleCounters::new(),
            read_counters: SimpleCounters::new(),
        }
    }

    fn phase_counters(&self, phase: &str) -> Option<&SimpleCounters> {
        match phase {
            SHUFFLE_WRITE_PHASE => Some(&self.write_counters),
            SHUFFLE_READ_PHASE => Some(&self.read_counters),
            _ => None,
        }
    }
}

impl RuntimeStats for ShuffleStats {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.base.add_duration_us(snapshot.duration_us());
        if let Some(phase) = &node_info.node_phase {
            if let Some(counters) = self.phase_counters(phase) {
                counters.add_duration_us(snapshot.duration_us());
            }
        }
        if let StatSnapshot::Default(s) = snapshot {
            self.base.add_rows_in(s.rows_in);
            self.base.add_rows_out(s.rows_out);
            // Don't track per-phase rows (see SortStats comment for rationale)
        }
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }

    fn export_phase_snapshots(&self) -> Vec<(usize, StatSnapshot)> {
        use crate::pipeline_node::phase_node_id;
        vec![
            (phase_node_id(self.node_id, 0), self.write_counters.export_default_snapshot()),
            (phase_node_id(self.node_id, 1), self.read_counters.export_default_snapshot()),
        ]
    }
}

impl PipelineNodeImpl for ShuffleNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(ShuffleStats::new(meter, self.context(), self.node_id()))
    }

    fn phases(&self) -> &[&str] {
        &[SHUFFLE_WRITE_PHASE, SHUFFLE_READ_PHASE]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let self_arc = self.clone();
        let local_shuffle_write_node = match &self.backend {
            DistributedShuffleBackend::Flight(backend) => {
                self.register_flight_cleanup(plan_context, backend);
                self.clone().build_flight_write_stage(input_node, backend)
            }
        };

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let flight_shuffle_execution = async move {
            self_arc
                .execution_loop(
                    local_shuffle_write_node,
                    task_id_counter,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        plan_context.spawn(flight_shuffle_execution);
        TaskBuilderStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![format!(
            "FlightShuffle: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
