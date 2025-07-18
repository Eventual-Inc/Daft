use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{partitioning::HashClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;

use super::{DistributedPipelineNode, RunningPipelineNode};
use crate::{
    pipeline_node::{
        make_in_memory_scan_from_materialized_outputs, MaterializedOutput, NodeID, NodeName,
        PipelineNodeConfig, PipelineNodeContext, PipelineOutput,
    },
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct RepartitionNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    columns: Vec<BoundExpr>,
    num_partitions: usize,
    child: Arc<dyn DistributedPipelineNode>,
}

impl RepartitionNode {
    const NODE_NAME: NodeName = "Repartition";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        logical_node_id: Option<NodeID>,
        stage_config: &StageConfig,
        columns: Vec<BoundExpr>,
        num_partitions: Option<usize>,
        schema: SchemaRef,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        let num_partitions =
            num_partitions.unwrap_or_else(|| child.config().clustering_spec.num_partitions());

        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![child.node_id()],
            vec![child.name()],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
            Arc::new(
                HashClusteringConfig::new(
                    num_partitions,
                    columns.clone().into_iter().map(|e| e.into()).collect(),
                )
                .into(),
            ),
        );

        Self {
            config,
            context,
            columns,
            num_partitions,
            child,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    fn multiline_display(&self) -> Vec<String> {
        use itertools::Itertools;
        let mut res = vec![];
        res.push(format!(
            "Repartition: {}",
            self.columns.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Num partitions = {}", self.num_partitions));
        res
    }

    pub(crate) async fn transpose_materialized_outputs(
        materialized_stream: impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
        num_partitions: usize,
    ) -> DaftResult<Vec<Vec<MaterializedOutput>>> {
        let materialized_partitions = materialized_stream
            .map(|mat| mat.map(|mat| mat.split_into_materialized_outputs()))
            .try_collect::<Vec<_>>()
            .await?;

        debug_assert!(
            materialized_partitions
                .iter()
                .all(|mat| mat.len() == num_partitions),
            "Expected all outputs to have {} partitions, got {}",
            num_partitions,
            materialized_partitions
                .iter()
                .map(|mat| mat.len())
                .join(", ")
        );

        let mut transposed_outputs = vec![];
        for idx in 0..num_partitions {
            let mut partition_group = vec![];
            for materialized_partition in &materialized_partitions {
                let part = &materialized_partition[idx];
                if part.num_rows()? > 0 {
                    partition_group.push(part.clone());
                }
            }
            transposed_outputs.push(partition_group);
        }

        assert_eq!(transposed_outputs.len(), num_partitions);
        Ok(transposed_outputs)
    }

    // Async execution to get all partitions out
    async fn execution_loop(
        self: Arc<Self>,
        local_repartition_node: RunningPipelineNode,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Trigger materialization of the partitions
        // This will produce a stream of materialized outputs, each containing a vector of num_partitions partitions
        let materialized_stream = local_repartition_node.materialize(scheduler_handle.clone());
        let transposed_outputs =
            Self::transpose_materialized_outputs(materialized_stream, self.num_partitions).await?;

        // Make each partition group (partitions equal by (hash % num_partitions)) input to a in-memory scan
        for partition_group in transposed_outputs {
            let self_clone = self.clone();
            let task = make_in_memory_scan_from_materialized_outputs(
                TaskContext::from((&self_clone.context, task_id_counter.next())),
                partition_group,
                &(self_clone as Arc<dyn DistributedPipelineNode>),
            )?;

            let _ = result_tx.send(PipelineOutput::Task(task)).await;
        }

        Ok(())
    }
}

impl TreeDisplay for RepartitionNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}

impl DistributedPipelineNode for RepartitionNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![self.child.clone()]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let input_node = self.child.clone().start(stage_context);

        // First pipeline the local repartition op
        let self_clone = self.clone();
        let local_repartition_node =
            input_node.pipeline_instruction(stage_context, self.clone(), move |input| {
                Ok(LocalPhysicalPlan::repartition(
                    input,
                    self_clone.columns.clone(),
                    self_clone.num_partitions,
                    self_clone.config.schema.clone(),
                    StatsState::NotMaterialized,
                ))
            });

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(
            local_repartition_node,
            stage_context.task_id_counter(),
            result_tx,
            stage_context.scheduler_handle(),
        );
        stage_context.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
