use std::{collections::VecDeque, pin::pin, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use futures::StreamExt;
use tracing::info_span;

use crate::{
    buffer::buffered_by_morsel_size,
    channel::{create_channel, Receiver},
    pipeline::PipelineNode,
    progress_bar::ProgressBarColor,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
    ExecutionRuntimeContext, ExecutionTaskSpawner, OperatorOutput, OrderableJoinSet,
    PipelineExecutionSnafu,
};

pub(crate) trait IntermediateOpState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

struct DefaultIntermediateOperatorState {}
impl IntermediateOpState for DefaultIntermediateOperatorState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub enum IntermediateOperatorResult {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    HasMoreOutput(Arc<MicroPartition>),
}

pub(crate) type IntermediateOpExecuteResult =
    OperatorOutput<DaftResult<(Box<dyn IntermediateOpState>, IntermediateOperatorResult)>>;
pub trait IntermediateOperator: Send + Sync {
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult;
    fn name(&self) -> &'static str;
    fn multiline_display(&self) -> Vec<String>;
    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(DefaultIntermediateOperatorState {}))
    }
    /// The maximum number of concurrent workers that can be spawned for this operator.
    /// Each worker will has its own IntermediateOperatorState.
    /// This method should be overridden if the operator needs to limit the number of concurrent workers, i.e. UDFs with resource requests.
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    fn morsel_size(&self, runtime_handle: &ExecutionRuntimeContext) -> Option<usize> {
        Some(runtime_handle.default_morsel_size())
    }
}

pub struct IntermediateNode {
    intermediate_op: Arc<dyn IntermediateOperator>,
    children: Vec<Box<dyn PipelineNode>>,
    runtime_stats: Arc<RuntimeStatsContext>,
    plan_stats: StatsState,
}

impl IntermediateNode {
    pub(crate) fn new(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
        plan_stats: StatsState,
    ) -> Self {
        let rts = RuntimeStatsContext::new(intermediate_op.name());
        Self::new_with_runtime_stats(intermediate_op, children, rts, plan_stats)
    }

    pub(crate) fn new_with_runtime_stats(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
        runtime_stats: Arc<RuntimeStatsContext>,
        plan_stats: StatsState,
    ) -> Self {
        Self {
            intermediate_op,
            children,
            runtime_stats,
            plan_stats,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for IntermediateNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.intermediate_op.name()).unwrap();
            }
            level => {
                let multiline_display = self.intermediate_op.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                if matches!(level, DisplayLevel::Verbose) {
                    writeln!(display).unwrap();
                    let rt_result = self.runtime_stats.result();
                    rt_result.display(&mut display, true, true, true).unwrap();
                }
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children.iter().map(|v| v.as_tree_display()).collect()
    }
}

impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect()
    }

    fn name(&self) -> &'static str {
        self.intermediate_op.name()
    }

    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let mut child_result_receivers = Vec::with_capacity(self.children.len());
        let progress_bar = runtime_handle.make_progress_bar(
            self.name(),
            ProgressBarColor::Magenta,
            true,
            self.runtime_stats.clone(),
        );
        for child in &self.children {
            let child_result_receiver = child.start(maintain_order, runtime_handle)?;
            child_result_receivers.push(CountingReceiver::new(
                child_result_receiver,
                self.runtime_stats.clone(),
                progress_bar.clone(),
            ));
        }
        let op = self.intermediate_op.clone();
        let (destination_sender, destination_receiver) = create_channel(1);
        // let counting_sender =
        //     CountingSender::new(destination_sender, self.runtime_stats.clone(), progress_bar);
        let task_spawner = ExecutionTaskSpawner {
            runtime_ref: get_compute_runtime(),
            memory_manager: runtime_handle.memory_manager(),
            runtime_context: self.runtime_stats.clone(),
            outer_span: info_span!("intermediate_op"),
        };
        let name = op.name();
        let morsel_size = op.morsel_size(runtime_handle);
        let stats = self.runtime_stats.clone();
        let progress_bar = progress_bar.clone();
        runtime_handle.spawn(
            async move {
                let flattened_stream = pin!(futures::stream::iter(
                    child_result_receivers.into_iter().map(|v| v.into_stream()),
                )
                .flatten());

                let mut buffered_stream = buffered_by_morsel_size(flattened_stream, morsel_size);

                let mut pending_tasks = OrderableJoinSet::new(true);
                let mut states = (0..op.max_concurrency())
                    .map(|_| op.make_state())
                    .collect::<DaftResult<VecDeque<_>>>()?;

                let sender = destination_sender.into_inner();
                loop {
                    let num_pending = pending_tasks.num_pending();
                    let can_spawn = !states.is_empty();
                    let output_task = async {
                        if let Ok(permit) = sender.reserve().await {
                            let finished_task =
                                pending_tasks.join_next().await.expect("No finished task");
                            let (state, result) = finished_task???;
                            match result {
                                IntermediateOperatorResult::NeedMoreInput(Some(output)) => {
                                    states.push_back(state);
                                    stats.mark_rows_emitted(output.len() as u64);
                                    if let Some(ref pb) = progress_bar {
                                        pb.render();
                                    }
                                    permit.send(output);
                                }
                                IntermediateOperatorResult::NeedMoreInput(None) => {
                                    states.push_back(state);
                                }
                                IntermediateOperatorResult::HasMoreOutput(output) => {
                                    pending_tasks.spawn_first(op.execute(
                                        output.clone(),
                                        state,
                                        &task_spawner,
                                    ));
                                    stats.mark_rows_emitted(output.len() as u64);
                                    if let Some(ref pb) = progress_bar {
                                        pb.render();
                                    }
                                    permit.send(output);
                                }
                            }
                            DaftResult::Ok(false)
                        } else {
                            DaftResult::Ok(true)
                        }
                    };

                    tokio::select! {
                        biased;
                        Some(new_input) = buffered_stream.next(), if can_spawn => {
                            let next_state = states.pop_front().unwrap();
                            pending_tasks.spawn(op.execute(new_input?, next_state, &task_spawner));
                        }
                        result = output_task, if num_pending > 0 => {
                            if result? {
                                return Ok(());
                            }
                        }
                        else => {
                            break;
                        }
                    }
                }
                assert!(buffered_stream.next().await.is_none());
                assert!(pending_tasks.num_pending() == 0);
                Ok(())
            },
            name,
        );
        Ok(destination_receiver)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
