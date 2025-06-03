use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{DistributedPipelineNode, MaterializedOutput, PipelineOutput, RunningPipelineNode};
use crate::{
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SchedulingStrategy, SwordfishTask},
    },
    stage::StageContext,
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
pub(crate) struct LimitNode {
    node_id: usize,
    limit: usize,
    schema: SchemaRef,
    config: Arc<DaftExecutionConfig>,
    child: Box<dyn DistributedPipelineNode>,
}

impl LimitNode {
    #[allow(dead_code)]
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

    async fn execution_loop(
        input: RunningPipelineNode,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        mut remaining_limit: usize,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        node_id: usize,
        schema: SchemaRef,
        config: Arc<DaftExecutionConfig>,
    ) -> DaftResult<()> {
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
                    let task_with_limit = make_task_with_limit(
                        materialized_output,
                        remaining_limit,
                        node_id,
                        schema.clone(),
                        config.clone(),
                    )?;
                    let task_result_handle = scheduler_handle.submit_task(task_with_limit).await?;
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
    fn name(&self) -> &'static str {
        "Limit"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![self.child.as_ref()]
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let input_node = self.child.start(stage_context);

        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = Self::execution_loop(
            input_node,
            result_tx,
            self.limit,
            stage_context.scheduler_handle.clone(),
            self.node_id,
            self.schema.clone(),
            self.config.clone(),
        );
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
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

    let mpset = HashMap::from([(node_id.to_string(), vec![partition])]);

    let task = SwordfishTask::new(
        limit_plan,
        config,
        mpset,
        SchedulingStrategy::WorkerAffinity {
            worker_id,
            soft: true,
        },
    );
    Ok(task)
}
