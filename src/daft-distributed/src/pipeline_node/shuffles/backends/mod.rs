use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{
    LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, ShuffleBackend, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{
        MaterializedOutput, NodeID, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
    },
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

#[cfg(feature = "celeborn")]
mod celeborn;
mod flight;
mod ray;

#[cfg(feature = "celeborn")]
use celeborn::CelebornShuffleReadSpec;

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

#[cfg(feature = "celeborn")]
static NEXT_CELEBORN_SHUFFLE_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "celeborn")]
fn next_celeborn_shuffle_id() -> u64 {
    NEXT_CELEBORN_SHUFFLE_ID.fetch_add(1, Ordering::Relaxed)
}

/// A shuffle node's resolved backend: the plan-level [`ShuffleBackend`] with this
/// node's `shuffle_id` stamped in, plus the node handles task building needs.
#[derive(Clone)]
pub(crate) struct ShuffleContext {
    backend: ShuffleBackend,
    schema: SchemaRef,
    node_id: NodeID,
}

impl ShuffleContext {
    #[cfg_attr(not(feature = "celeborn"), allow(unused_variables))]
    pub(crate) fn new(
        context: &PipelineNodeContext,
        schema: SchemaRef,
        backend: ShuffleBackend,
        input_num_partitions: usize,
    ) -> Self {
        Self {
            schema,
            node_id: context.node_id,
            backend: match backend {
                ShuffleBackend::Ray => ShuffleBackend::Ray,
                ShuffleBackend::Flight {
                    shuffle_dirs,
                    compression,
                    ..
                } => ShuffleBackend::Flight {
                    shuffle_id: make_shuffle_id(context),
                    shuffle_dirs,
                    compression,
                },
                #[cfg(feature = "celeborn")]
                ShuffleBackend::Celeborn { .. } => ShuffleBackend::Celeborn {
                    shuffle_id: next_celeborn_shuffle_id(),
                    num_mappers: input_num_partitions.max(1) as u32,
                },
            },
        }
    }

    pub(crate) fn backend(&self) -> &ShuffleBackend {
        &self.backend
    }

    pub(crate) fn build_map_stream<F>(
        &self,
        input: TaskBuilderStream,
        node: Arc<dyn PipelineNodeImpl>,
        plan_builder: F,
    ) -> TaskBuilderStream
    where
        F: Fn(LocalPhysicalPlanRef) -> LocalPhysicalPlanRef + Send + Sync + 'static,
    {
        self.prepare_map_tasks(input.pipeline_instruction(node, plan_builder))
    }

    fn prepare_map_tasks(&self, stream: TaskBuilderStream) -> TaskBuilderStream {
        match &self.backend {
            ShuffleBackend::Ray | ShuffleBackend::Flight { .. } => stream,
            #[cfg(feature = "celeborn")]
            ShuffleBackend::Celeborn { .. } => {
                use futures::StreamExt;
                TaskBuilderStream::new(
                    stream
                        .enumerate()
                        .map(|(map_id, builder)| {
                            builder.with_context_value("map_id", map_id.to_string())
                        })
                        .boxed(),
                )
            }
        }
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn node_id(&self) -> NodeID {
        self.node_id
    }

    pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext) {
        match &self.backend {
            ShuffleBackend::Ray => {}
            ShuffleBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                ..
            } => {
                flight::register_cleanup(*shuffle_id, shuffle_dirs, plan_context);
            }
            #[cfg(feature = "celeborn")]
            ShuffleBackend::Celeborn { .. } => {}
        }
    }

    /// Build a `SwordfishTaskBuilder` whose plan reads from already-materialized
    /// partition refs (`in_memory_scan` for Ray, `shuffle_read(Flight)` for Flight)
    /// and then applies `wrap_plan` on top. The partition refs are attached to
    /// the task via the backend-appropriate API (`with_psets` /
    /// `with_flight_shuffle_reads`).
    pub(crate) fn build_refs_task_builder<F>(
        &self,
        partition_refs: Vec<PartitionRef>,
        node: &dyn PipelineNodeImpl,
        wrap_plan: F,
    ) -> SwordfishTaskBuilder
    where
        F: FnOnce(LocalPhysicalPlanRef) -> LocalPhysicalPlanRef,
    {
        let node_id = self.node_id;
        match &self.backend {
            ShuffleBackend::Ray => {
                let total_size_bytes = partition_refs.iter().map(|p| p.size_bytes()).sum::<usize>();
                let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
                    node_id,
                    self.schema.clone(),
                    total_size_bytes,
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                );
                let plan = wrap_plan(in_memory_scan);
                SwordfishTaskBuilder::new(plan, node, node_id).with_psets(node_id, partition_refs)
            }
            ShuffleBackend::Flight { .. } => {
                let read_inputs = flight::read_inputs_from_refs(partition_refs);
                let shuffle_read = LocalPhysicalPlan::shuffle_read(
                    node_id,
                    self.schema.clone(),
                    ShuffleReadBackend::Flight {
                        shuffle_id: 0,
                        server_cache_mapping: Default::default(),
                    },
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                );
                let plan = wrap_plan(shuffle_read);
                SwordfishTaskBuilder::new(plan, node, node_id)
                    .with_flight_shuffle_reads(node_id, read_inputs)
            }
            #[cfg(feature = "celeborn")]
            ShuffleBackend::Celeborn { .. } => {
                unreachable!("Celeborn reads are built from shuffle metadata")
            }
        }
    }

    pub(crate) async fn emit_gather_read_task(
        &self,
        materialized: Vec<MaterializedOutput>,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match &self.backend {
            ShuffleBackend::Ray | ShuffleBackend::Flight { .. } => {
                let refs = materialized
                    .into_iter()
                    .flat_map(|output| output.into_inner().0)
                    .collect();
                let task = self.build_refs_task_builder(refs, node, |plan| plan);
                let _ = result_tx.send(task).await;
                Ok(())
            }
            #[cfg(feature = "celeborn")]
            ShuffleBackend::Celeborn {
                shuffle_id,
                num_mappers,
            } => {
                let read_spec = CelebornShuffleReadSpec {
                    shuffle_id: *shuffle_id,
                    num_mappers: *num_mappers,
                };
                celeborn::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    1,
                    read_spec,
                    node,
                    result_tx,
                )
                .await
            }
        }
    }

    #[cfg(feature = "celeborn")]
    pub(crate) async fn emit_celeborn_read_tasks_with<F>(
        &self,
        mut materialized_stream: impl futures::Stream<Item = DaftResult<MaterializedOutput>>
        + Send
        + Unpin,
        num_partitions: usize,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
        wrap_plan: F,
    ) -> DaftResult<()>
    where
        F: FnMut(usize, LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef>,
    {
        use futures::StreamExt;
        while let Some(output) = materialized_stream.next().await {
            output?;
        }
        let ShuffleBackend::Celeborn {
            shuffle_id,
            num_mappers,
        } = &self.backend
        else {
            unreachable!("Celeborn read task builder used with another backend")
        };
        celeborn::emit_read_tasks_with(
            self.node_id,
            self.schema.clone(),
            num_partitions,
            CelebornShuffleReadSpec {
                shuffle_id: *shuffle_id,
                num_mappers: *num_mappers,
            },
            node,
            result_tx,
            wrap_plan,
        )
        .await
    }

    /// Group a stream of map-task outputs into per-partition read tasks.
    ///
    /// The Ray backend transposes the full (tasks x partitions) matrix of object refs.
    /// The flight backend folds the stream into per-server map-input lists shared by
    /// all reduce tasks, keeping coordinator memory O(map_tasks + partitions) instead
    /// of O(map_tasks x partitions).
    pub(crate) async fn emit_read_tasks_from_stream(
        &self,
        materialized_stream: impl futures::Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
        num_partitions: usize,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match &self.backend {
            ShuffleBackend::Ray => {
                let partition_groups =
                    crate::utils::transpose::transpose_materialized_outputs_from_stream(
                        materialized_stream,
                        num_partitions,
                    )
                    .await?;
                ray::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            ShuffleBackend::Flight { shuffle_id, .. } => {
                let read_inputs = flight::fold_outputs_from_stream(
                    materialized_stream,
                    num_partitions,
                    *shuffle_id,
                )
                .await?;
                flight::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    read_inputs,
                    node,
                    result_tx,
                )
                .await
            }
            #[cfg(feature = "celeborn")]
            ShuffleBackend::Celeborn {
                shuffle_id,
                num_mappers,
            } => {
                use futures::StreamExt;
                let mut materialized_stream = materialized_stream;
                while let Some(output) = materialized_stream.next().await {
                    output?;
                }
                let read_spec = CelebornShuffleReadSpec {
                    shuffle_id: *shuffle_id,
                    num_mappers: *num_mappers,
                };
                celeborn::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    num_partitions,
                    read_spec,
                    node,
                    result_tx,
                )
                .await
            }
        }
    }
}
use std::sync::Arc;
#[cfg(feature = "celeborn")]
use std::sync::atomic::{AtomicU64, Ordering};
