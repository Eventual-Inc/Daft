use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::{atomic::AtomicUsize, Arc, Mutex},
};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{
    materialize::materialize_pipeline_results, translate::PipelinePlan, DistributedPipelineNode,
    PipelineOutput, RunningPipelineNode,
};
use crate::{
    channel::{create_channel, Receiver, Sender},
    runtime::{JoinSet, OrderedJoinSet},
    scheduling::{
        dispatcher::{SubmittedTask, TaskDispatcherHandle},
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::StageContext,
};

#[allow(dead_code)]
pub(crate) struct LimitNode {
    node_id: usize,
    limit: usize,
    schema: SchemaRef,
    child: Box<dyn DistributedPipelineNode>,
}

impl LimitNode {
    #[allow(dead_code)]
    pub fn new(
        node_id: usize,
        limit: usize,
        schema: SchemaRef,
        child: Box<dyn DistributedPipelineNode>,
    ) -> Self {
        Self {
            node_id,
            limit,
            schema,
            child,
        }
    }

    async fn limit_execution_loop(
        mut materialized_result_rx: Receiver<PartitionRef>,
        result_tx: Sender<PipelineOutput>,
        mut remaining_limit: usize,
        task_dispatcher_handle: TaskDispatcherHandle,
        node_id: usize,
        schema: SchemaRef,
    ) -> DaftResult<()> {
        while let Some(partition_ref) = materialized_result_rx.recv().await {
            let num_rows = partition_ref.num_rows()?;
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

impl TreeDisplay for LimitNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
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

impl DistributedPipelineNode for LimitNode {
    fn name(&self) -> &'static str {
        "Limit"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![self.child.as_ref()]
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let task_dispatcher_handle = stage_context.get_task_dispatcher_handle();
        let input_node = self.child.start(stage_context);
        let materialized_result_rx = materialize_pipeline_results(input_node, stage_context);
        let (tx, rx) = create_channel(1);
        stage_context.spawn_task_on_joinset(Self::limit_execution_loop(
            materialized_result_rx,
            tx,
            self.limit,
            task_dispatcher_handle,
            self.node_id,
            self.schema.clone(),
        ));
        RunningPipelineNode::new(rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

fn make_task_with_limit(
    partition_ref: PartitionRef,
    limit: usize,
    node_id: usize,
    schema: SchemaRef,
) -> DaftResult<SwordfishTask> {
    let in_memory_info = InMemoryInfo::new(schema, node_id.to_string(), None, 1, 0, 0, None, None);

    let in_memory_source =
        LocalPhysicalPlan::in_memory_scan(in_memory_info, StatsState::NotMaterialized);

    let limit_plan =
        LocalPhysicalPlan::limit(in_memory_source, limit as i64, StatsState::NotMaterialized);

    let mut mpset = HashMap::new();
    mpset.insert(node_id.to_string(), vec![partition_ref]);

    let task = SwordfishTask::new(limit_plan, mpset, SchedulingStrategy::Spread);
    Ok(task)
}
