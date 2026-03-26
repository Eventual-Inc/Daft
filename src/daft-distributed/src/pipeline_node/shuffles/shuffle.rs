use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{
    FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend,
    ShuffleWriteBackend,
};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, ShufflePartitionRef, TaskBuilderStream, TaskOutput,
    },
    plan::{PlanConfig, PlanExecutionContext, QueryIdx, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_stream,
    },
};

fn make_shuffle_id(query_idx: QueryIdx, node_id: NodeID) -> u64 {
    ((query_idx as u64) << 32) | (node_id as u64)
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
    Ray,
    Flight(FlightDistributedShuffleConfig),
}

impl DistributedShuffleBackend {
    fn display_name(&self) -> &'static str {
        match self {
            Self::Ray => "ray",
            Self::Flight(_) => "flight",
        }
    }
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
    pub fn new_ray(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        child: DistributedPipelineNode,
    ) -> Self {
        Self::new(
            node_id,
            plan_config,
            repartition_spec,
            schema,
            num_partitions,
            DistributedShuffleBackend::Ray,
            child,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_flight(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
        child: DistributedPipelineNode,
    ) -> Self {
        let shuffle_id = make_shuffle_id(plan_config.query_idx, node_id);

        Self::new(
            node_id,
            plan_config,
            repartition_spec,
            schema,
            num_partitions,
            DistributedShuffleBackend::Flight(FlightDistributedShuffleConfig {
                shuffle_id,
                shuffle_dirs,
                compression,
            }),
            child,
        )
    }

    fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        backend: DistributedShuffleBackend,
        child: DistributedPipelineNode,
    ) -> Self {
        let node_name = format!("Repartition ({})", backend.display_name());
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(node_name),
            NodeType::Repartition,
            NodeCategory::BlockingSink,
        );
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
            backend,
            child,
        }
    }

    fn build_ray_repartition_stage(
        self: Arc<Self>,
        input_node: TaskBuilderStream,
    ) -> TaskBuilderStream {
        let self_clone = self.clone();
        input_node.pipeline_instruction(self, move |input| {
            LocalPhysicalPlan::repartition(
                input,
                self_clone.repartition_spec.clone(),
                self_clone.num_partitions,
                self_clone.config.schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self_clone.node_id() as usize)),
            )
        })
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
                LocalNodeContext::new(Some(self.context.node_id as usize)),
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
                LocalNodeContext::new(Some(self.context.node_id as usize)),
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

    async fn emit_ray_read_tasks(
        &self,
        transposed_outputs: Vec<Vec<MaterializedOutput>>,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        for partition_group in transposed_outputs {
            let (in_memory_source_plan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                partition_group,
                self.config.schema.clone(),
                self.node_id(),
            );
            let builder = SwordfishTaskBuilder::new(in_memory_source_plan, self, self.node_id())
                .with_psets(self.node_id(), psets);

            let _ = result_tx.send(builder).await;
        }

        Ok(())
    }

    async fn execution_loop_flight(
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

        let DistributedShuffleBackend::Flight(backend) = &self.backend else {
            unreachable!("Flight execution loop invoked with non-flight backend");
        };
        let read_spec = self.flight_read_spec_from_outputs(backend, outputs)?;
        self.emit_flight_read_tasks(read_spec, result_tx).await?;

        Ok(())
    }

    async fn execution_loop_ray(
        self: Arc<Self>,
        local_repartition_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let materialized_stream = local_repartition_node.materialize(
            scheduler_handle,
            self.context.query_idx,
            task_id_counter,
        );
        let transposed_outputs =
            transpose_materialized_outputs_from_stream(materialized_stream, self.num_partitions)
                .await?;

        self.emit_ray_read_tasks(transposed_outputs, result_tx)
            .await
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

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);
        match &self.backend {
            DistributedShuffleBackend::Ray => {
                let local_repartition_node = self.clone().build_ray_repartition_stage(input_node);
                let self_arc = self.clone();
                let task_id_counter = plan_context.task_id_counter();
                let scheduler_handle = plan_context.scheduler_handle();
                plan_context.spawn(async move {
                    self_arc
                        .execution_loop_ray(
                            local_repartition_node,
                            task_id_counter,
                            result_tx,
                            scheduler_handle,
                        )
                        .await
                });
            }
            DistributedShuffleBackend::Flight(backend) => {
                self.register_flight_cleanup(plan_context, backend);
                let local_shuffle_write_node =
                    self.clone().build_flight_write_stage(input_node, backend);
                let self_arc = self.clone();
                let task_id_counter = plan_context.task_id_counter();
                let scheduler_handle = plan_context.scheduler_handle();
                plan_context.spawn(async move {
                    self_arc
                        .execution_loop_flight(
                            local_shuffle_write_node,
                            task_id_counter,
                            result_tx,
                            scheduler_handle,
                        )
                        .await
                });
            }
        }
        TaskBuilderStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![format!(
            "Repartition ({}): {}",
            self.backend.display_name(),
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
