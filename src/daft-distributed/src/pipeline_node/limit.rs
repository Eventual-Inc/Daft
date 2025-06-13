use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct LimitNode {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    limit: usize,
    schema: SchemaRef,
    config: Arc<DaftExecutionConfig>,
    child: Arc<dyn DistributedPipelineNode>,
}

impl LimitNode {
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: NodeID,
        limit: usize,
        schema: SchemaRef,
        config: Arc<DaftExecutionConfig>,
        child: Arc<dyn DistributedPipelineNode>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            limit,
            schema,
            config,
            child,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn execution_loop(
        self,
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        context: HashMap<String, String>,
    ) -> DaftResult<()> {
        let mut remaining_limit = self.limit;
        let mut materialized_result_stream = input.materialize(scheduler_handle.clone());

        while let Some(materialized_output) = materialized_result_stream.next().await {
            let materialized_output = materialized_output?;
            let num_rows = materialized_output.partition().num_rows()?;

            let (to_send, should_break) = match num_rows.cmp(&remaining_limit) {
                Ordering::Less => {
                    remaining_limit -= num_rows;
                    (PipelineOutput::Materialized(materialized_output), false)
                }
                Ordering::Equal => (PipelineOutput::Materialized(materialized_output), true),
                Ordering::Greater => {
                    let task_with_limit = self.make_task_with_limit(
                        materialized_output,
                        context.clone(),
                        remaining_limit,
                    )?;
                    let task_result_handle = task_with_limit.submit(&scheduler_handle).await?;
                    (PipelineOutput::Running(task_result_handle), true)
                }
            };
            if result_tx.send(to_send).await.is_err() {
                break;
            }
            if should_break {
                break;
            }
        }
        Ok(())
    }

    fn make_task_with_limit(
        &self,
        materialized_output: MaterializedOutput,
        context: HashMap<String, String>,
        limit: usize,
    ) -> DaftResult<SubmittableTask<SwordfishTask>> {
        let (partition, worker_id) = materialized_output.into_inner();
        let in_memory_info = InMemoryInfo::new(
            self.schema.clone(),
            self.node_id.to_string(),
            None,
            1,
            0,
            0,
            None,
            None,
        );

        let in_memory_source =
            LocalPhysicalPlan::in_memory_scan(in_memory_info, StatsState::NotMaterialized);

        let limit_plan =
            LocalPhysicalPlan::limit(in_memory_source, limit as i64, StatsState::NotMaterialized);

        let mpset = HashMap::from([(self.node_id.to_string(), vec![partition])]);

        let task = SwordfishTask::new(
            limit_plan,
            self.config.clone(),
            mpset,
            SchedulingStrategy::WorkerAffinity {
                worker_id,
                soft: true,
            },
            context,
            self.node_id,
        );
        Ok(SubmittableTask::new(task))
    }
}

impl TreeDisplay for LimitNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id).unwrap();
        writeln!(display, "Limit: {}", self.limit).unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}

impl DistributedPipelineNode for LimitNode {
    fn name(&self) -> &'static str {
        "DistributedLimit"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![self.child.as_ref()]
    }

    fn start(&self, stage_context: &mut StageContext) -> RunningPipelineNode {
        // let child_id = self.child..node_id();
        let child_name = self.child.name();
        let child_id = self.child.node_id();

        let context = HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), format!("{}", self.stage_id)),
            ("node_id".to_string(), format!("{}", self.node_id)),
            ("node_name".to_string(), self.name().to_string()),
            ("child_id".to_string(), format!("{}", child_id)),
            ("child_name".to_string(), child_name.to_string()),
        ]);

        let input_node = self.child.start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.clone().execution_loop(
            input_node,
            result_tx,
            stage_context.scheduler_handle.clone(),
            context,
        );
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
    fn plan_id(&self) -> &PlanID {
        &self.plan_id
    }

    fn stage_id(&self) -> &StageID {
        &self.stage_id
    }

    fn node_id(&self) -> &NodeID {
        &self.node_id
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
