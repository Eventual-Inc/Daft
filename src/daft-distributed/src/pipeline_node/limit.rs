use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    scheduling::{
        dispatcher::TaskDispatcherHandleRef,
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::StageContext,
    utils::{
        channel::{create_channel, Sender},
        joinset::JoinSet,
    },
};

#[derive(Debug)]
pub(crate) struct LimitNode {
    node_id: usize,
    limit: usize,
    schema: SchemaRef,
    config: Arc<DaftExecutionConfig>,
    child: Box<dyn DistributedPipelineNode>,
}

impl LimitNode {
    pub fn new(
        node_id: usize,
        limit: usize,
        schema: SchemaRef,
        config: Arc<DaftExecutionConfig>,
        child: Box<dyn DistributedPipelineNode>,
    ) -> Self {
        Self {
            node_id,
            limit,
            schema,
            config,
            child,
        }
    }

    async fn limit_execution_loop(
        input: RunningPipelineNode<SwordfishTask>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        mut remaining_limit: usize,
        task_dispatcher_handle: TaskDispatcherHandleRef<SwordfishTask>,
        node_id: usize,
        schema: SchemaRef,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<()> {
        let mut materialized_result_stream = input.materialize(task_dispatcher_handle.clone());
        while let Some(partition_ref) = materialized_result_stream.next().await {
            let partition_ref = partition_ref?;
            let num_rows = partition_ref.partition().num_rows()?;
            let (to_send, should_break) = match num_rows.cmp(&remaining_limit) {
                Ordering::Less => {
                    remaining_limit -= num_rows;
                    (PipelineOutput::Materialized(partition_ref), false)
                }
                Ordering::Equal => (PipelineOutput::Materialized(partition_ref), true),
                Ordering::Greater => {
                    let task_with_limit = make_task_with_limit(
                        partition_ref,
                        remaining_limit,
                        node_id,
                        schema.clone(),
                        config.clone(),
                    )?;
                    let task_result_handle =
                        task_dispatcher_handle.submit_task(task_with_limit).await?;
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
}

impl DistributedPipelineNode for LimitNode {
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn name(&self) -> &'static str {
        "Limit"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![self.child.as_ref()]
    }

    fn start(
        &self,
        stage_context: &mut StageContext,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> RunningPipelineNode<SwordfishTask> {
        let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
        let input_node = self.child.start(stage_context, psets);

        let (tx, rx) = create_channel(1);
        stage_context.spawn_task_on_joinset(Self::limit_execution_loop(
            input_node,
            tx,
            self.limit,
            task_dispatcher_handle,
            self.node_id,
            self.schema.clone(),
            self.config.clone(),
        ));
        RunningPipelineNode::new(rx)
    }
}

impl TreeDisplay for LimitNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", "Limit").unwrap();
        writeln!(display, "Limit: {}", self.limit).unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

fn make_task_with_limit(
    materialized_output: MaterializedOutput,
    limit: usize,
    node_id: usize,
    schema: SchemaRef,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<SwordfishTask> {
    let (partition, worker_id) = materialized_output.into_inner();
    let in_memory_info = InMemoryInfo::new(schema, node_id.to_string(), None, 1, 0, 0, None, None);

    let in_memory_source =
        LocalPhysicalPlan::in_memory_scan(in_memory_info, StatsState::NotMaterialized);

    let limit_plan =
        LocalPhysicalPlan::limit(in_memory_source, limit as i64, StatsState::NotMaterialized);

    let mut mpset = HashMap::new();
    mpset.insert(node_id.to_string(), vec![partition]);

    let task = SwordfishTask::new(
        limit_plan,
        config,
        mpset,
        SchedulingStrategy::NodeAffinity {
            node_id: worker_id,
            soft: true,
        },
    );
    Ok(task)
}
