use std::{
    hash::{DefaultHasher, Hash as _, Hasher as _},
    sync::Arc,
};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_functions::random::random_int_expr;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleBackend};
use daft_logical_plan::{partitioning::RandomShuffleConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        ClusteringStrategy, DistributedPipelineNode, NodeID, PipelineNodeConfig,
        PipelineNodeContext, shuffles::backends::ShuffleContext,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct RandomShuffleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    seed: Option<u64>,
    shuffle_context: ShuffleContext,
    child: DistributedPipelineNode,
}

impl RandomShuffleNode {
    const NODE_NAME: &'static str = "RandomShuffle";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        seed: Option<u64>,
        output_schema: SchemaRef,
        backend: ShuffleBackend,
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
            ClusteringStrategy::Passthrough { child: &child },
        );
        let shuffle_context = ShuffleContext::new(&context, output_schema, backend);
        Self {
            config,
            context,
            seed,
            shuffle_context,
            child,
        }
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

        let seed = self.seed;
        let schema = self.config.schema.clone();
        let node_id = self.node_id();
        self.shuffle_context
            .emit_read_tasks_from_stream(
                outputs,
                num_partitions,
                self.as_ref(),
                result_tx,
                &mut |partition_idx, input| {
                    let partition_seed = seed.map(|s| {
                        let mut hasher = DefaultHasher::new();
                        s.hash(&mut hasher);
                        partition_idx.hash(&mut hasher);
                        hasher.finish()
                    });
                    let sort_by = BoundExpr::bind_all(
                        &[random_int_expr(i64::MIN, i64::MAX, partition_seed)],
                        &schema,
                    )?;
                    Ok(LocalPhysicalPlan::sort(
                        input,
                        sort_by,
                        vec![false],
                        vec![false],
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(node_id as usize)),
                    ))
                },
            )
            .await
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
        let backend_name = self.shuffle_context.backend().name();
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
        self.shuffle_context.register_cleanup(plan_context);

        let num_partitions = self.child.config().clustering_spec.num_partitions();
        let node_id = self.node_id();
        let schema = self.config.schema.clone();
        let seed = self.seed;
        let shuffle_backend = self.shuffle_context.backend().clone();

        let partitioned_input = input_node.pipeline_instruction(self.clone(), move |input| {
            LocalPhysicalPlan::repartition_write(
                input,
                num_partitions,
                schema.clone(),
                shuffle_backend.clone(),
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
