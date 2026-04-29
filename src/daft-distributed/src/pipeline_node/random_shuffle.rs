use std::{
    hash::{DefaultHasher, Hash as _, Hasher as _},
    sync::Arc,
};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions::random::random_int_expr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::RandomShuffleConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, PipelineNodeConfig,
        PipelineNodeContext,
        shuffles::backends::{DistributedShuffleBackend, ShuffleBackend},
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::{
        channel::{Sender, create_channel},
        transpose::transpose_materialized_outputs_from_stream,
    },
};

pub(crate) struct RandomShuffleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    seed: Option<u64>,
    shuffle_backend: ShuffleBackend,
    child: DistributedPipelineNode,
}

impl RandomShuffleNode {
    const NODE_NAME: &'static str = "RandomShuffle";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        seed: Option<u64>,
        output_schema: SchemaRef,
        backend: DistributedShuffleBackend,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::RandomShuffle,
            NodeCategory::BlockingSink,
        );

        let config = PipelineNodeConfig::new(
            output_schema.clone(),
            plan_config.config.clone(),
            child.config().clustering_spec.clone(),
        );
        let shuffle_backend = ShuffleBackend::new(&context, output_schema, backend);
        Self {
            config,
            context,
            seed,
            shuffle_backend,
            child,
        }
    }

    fn local_sort_with_random_key(
        &self,
        partition_group: Vec<MaterializedOutput>,
        partition_idx: usize,
    ) -> DaftResult<SwordfishTaskBuilder> {
        let partition_refs = partition_group
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();

        let partition_seed = self.seed.map(|s| {
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            partition_idx.hash(&mut hasher);
            hasher.finish()
        });

        let sort_by = BoundExpr::bind_all(
            &[random_int_expr(i64::MIN, i64::MAX, partition_seed)],
            &self.config.schema,
        )?;
        let node_id = self.node_id();
        Ok(self
            .shuffle_backend
            .build_refs_task_builder(partition_refs, self, |input| {
                LocalPhysicalPlan::sort(
                    input,
                    sort_by,
                    vec![false],
                    vec![false],
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                )
            }))
    }

    async fn execution_loop(
        self: Arc<Self>,
        input_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let num_partitions = self.child.config().clustering_spec.num_partitions();
        let outputs = input_node.materialize(
            scheduler_handle.clone(),
            self.context.query_idx,
            task_id_counter.clone(),
        );

        let partition_groups =
            transpose_materialized_outputs_from_stream(outputs, num_partitions).await?;

        for (partition_idx, partition_group) in partition_groups.into_iter().enumerate() {
            let task = self.local_sort_with_random_key(partition_group, partition_idx)?;
            let _ = result_tx.send(task).await;
        }
        Ok(())
    }
}

impl PipelineNodeImpl for RandomShuffleNode {
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
        let backend_name = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => "Ray",
            DistributedShuffleBackend::Flight(_) => "Flight",
        };
        vec![
            format!(
                "RandomShuffle({}): random row order (via random repartition + random_int + sort)",
                backend_name
            ),
            format!("Seed = {:?}", self.seed),
        ]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        self.shuffle_backend.register_cleanup(plan_context);

        let num_partitions = self.child.config().clustering_spec.num_partitions();
        let node_id = self.node_id();
        let schema = self.config.schema.clone();
        let seed = self.seed;
        let local_shuffle_backend = self.shuffle_backend.local_shuffle_backend();

        let partitioned_input = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::repartition_write(
                input,
                num_partitions,
                schema.clone(),
                local_shuffle_backend.clone(),
                daft_logical_plan::partitioning::RepartitionSpec::Random(
                    RandomShuffleConfig::new_with_seed(Some(num_partitions), seed),
                ),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        });

        let (result_tx, result_rx) = create_channel(1);
        plan_context.spawn(self.execution_loop(
            partitioned_input,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));
        TaskBuilderStream::from(result_rx)
    }
}
