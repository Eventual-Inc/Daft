use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::{NodeCategory, NodeType};
use common_partitioning::PartitionRef;
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
    utils::channel::{Sender, create_channel},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

pub(crate) struct FlightDistributedShuffleConfig {
    pub(crate) shuffle_id: u64,
    pub(crate) shuffle_dirs: Vec<String>,
    pub(crate) compression: Option<String>,
}

struct FlightReadSpec {
    shuffle_id: u64,
    server_cache_mapping: HashMap<String, Vec<u32>>,
}

pub(crate) enum DistributedShuffleBackend {
    Ray,
    Flight(FlightDistributedShuffleConfig),
}

pub(crate) fn ray_partition_groups_from_outputs(
    outputs: Vec<TaskOutput>,
    num_partitions: usize,
) -> DaftResult<Vec<Vec<PartitionRef>>> {
    let mut partition_groups = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();

    for output in outputs {
        let TaskOutput::ShuffleWrite(output) = output else {
            return Err(DaftError::InternalError(
                "Expected Ray shuffle write task output".to_string(),
            ));
        };

        if output.partitions.len() != num_partitions {
            return Err(DaftError::InternalError(format!(
                "Expected {} Ray shuffle partitions, got {}",
                num_partitions,
                output.partitions.len()
            )));
        }

        for (partition_idx, partition) in output.partitions.into_iter().enumerate() {
            match partition {
                ShufflePartitionRef::Ray(partition) => {
                    if partition.num_rows() > 0 {
                        partition_groups[partition_idx].push(partition);
                    }
                }
                ShufflePartitionRef::Flight(_) => {
                    return Err(DaftError::InternalError(
                        "Expected Ray shuffle partition ref but received Flight".to_string(),
                    ));
                }
            }
        }
    }

    Ok(partition_groups)
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
    const NODE_NAME: &'static str = "Shuffle";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        backend: DistributedShuffleBackend,
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
            backend: match backend {
                DistributedShuffleBackend::Ray => DistributedShuffleBackend::Ray,
                DistributedShuffleBackend::Flight(backend) => {
                    DistributedShuffleBackend::Flight(FlightDistributedShuffleConfig {
                        shuffle_id,
                        shuffle_dirs: backend.shuffle_dirs,
                        compression: backend.compression,
                    })
                }
            },
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
                LocalNodeContext::new(Some(self.context.node_id as usize)),
            )
        })
    }

    fn build_ray_write_stage(self: Arc<Self>, input_node: TaskBuilderStream) -> TaskBuilderStream {
        let repartition_spec = self.repartition_spec.clone();
        input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::shuffle_write(
                input,
                None,
                self.num_partitions,
                self.config.schema.clone(),
                ShuffleWriteBackend::Ray {
                    repartition_spec: repartition_spec.clone(),
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
                    ShufflePartitionRef::Ray(_) => {
                        return Err(DaftError::InternalError(
                            "Expected Flight shuffle partition ref but received Ray".to_string(),
                        ));
                    }
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
        partition_groups: Vec<Vec<PartitionRef>>,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        for partition_group in partition_groups {
            let total_size_bytes = partition_group
                .iter()
                .map(|p| p.size_bytes())
                .sum::<usize>();
            let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
                self.node_id(),
                self.config.schema.clone(),
                total_size_bytes,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.node_id() as usize)),
            );

            let builder = SwordfishTaskBuilder::new(in_memory_scan, self, self.node_id())
                .with_psets(self.node_id(), partition_group);

            let _ = result_tx.send(builder).await;
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
            DistributedShuffleBackend::Ray => {
                let partition_groups =
                    ray_partition_groups_from_outputs(outputs, self.num_partitions)?;
                self.emit_ray_read_tasks(partition_groups, result_tx)
                    .await?;
            }
            DistributedShuffleBackend::Flight(backend) => {
                let read_spec = self.flight_read_spec_from_outputs(backend, outputs)?;
                self.emit_flight_read_tasks(read_spec, result_tx).await?;
            }
        }

        Ok(())
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
        let self_arc = self.clone();
        let local_shuffle_write_node = match &self.backend {
            DistributedShuffleBackend::Ray => self.clone().build_ray_write_stage(input_node),
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
        let backend_name = match &self.backend {
            DistributedShuffleBackend::Ray => "RayShuffle",
            DistributedShuffleBackend::Flight(_) => "FlightShuffle",
        };
        let mut res = vec![format!(
            "{backend_name}: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
