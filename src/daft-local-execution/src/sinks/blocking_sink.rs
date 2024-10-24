use std::sync::{Arc, Mutex};

use common_daft_config::DaftExecutionConfig;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::{info_span, instrument};

use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    pipeline::{PipelineNode, PipelineResultType},
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, TaskSet,
};
pub trait DynBlockingSinkState: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub(crate) struct BlockingSinkState {
    inner: Mutex<Box<dyn DynBlockingSinkState>>,
}

impl BlockingSinkState {
    fn new(inner: Box<dyn DynBlockingSinkState>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(inner),
        })
    }

    pub(crate) fn with_state_mut<T: DynBlockingSinkState + 'static, F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.inner.lock().unwrap();
        let state = guard
            .as_any_mut()
            .downcast_mut::<T>()
            .expect("State type mismatch");
        f(state)
    }
}

pub enum BlockingSinkStatus {
    NeedMoreInput,
    #[allow(dead_code)]
    Finished,
}

pub trait BlockingSink: Send + Sync {
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state_handle: &BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus>;
    fn finalize(
        &self,
        states: Vec<Box<dyn DynBlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>>;
    fn name(&self) -> &'static str;
    fn make_state(&self) -> DaftResult<Box<dyn DynBlockingSinkState>>;
    fn max_concurrency(&self) -> usize;
    fn morsel_size(&self) -> Option<usize> {
        Some(DaftExecutionConfig::default().default_morsel_size)
    }
}

pub struct BlockingSinkNode {
    op: Arc<dyn BlockingSink>,
    name: &'static str,
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
}

impl BlockingSinkNode {
    pub(crate) fn new(op: Arc<dyn BlockingSink>, child: Box<dyn PipelineNode>) -> Self {
        let name = op.name();
        Self {
            op,
            name,
            child,
            runtime_stats: RuntimeStatsContext::new(),
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    #[instrument(level = "info", skip_all, name = "BlockingSink::run_worker")]
    async fn run_worker(
        op: Arc<dyn BlockingSink>,
        mut input_receiver: Receiver<PipelineResultType>,
        rt_context: Arc<RuntimeStatsContext>,
    ) -> DaftResult<Box<dyn DynBlockingSinkState>> {
        let span = info_span!("BlockingSink::Sink");
        let compute_runtime = get_compute_runtime();
        let state_wrapper = BlockingSinkState::new(op.make_state()?);
        while let Some(morsel) = input_receiver.recv().await {
            let op = op.clone();
            let morsel = morsel.clone();
            let span = span.clone();
            let rt_context = rt_context.clone();
            let state_wrapper = state_wrapper.clone();
            let fut = async move {
                rt_context.in_span(&span, || op.sink(morsel.as_data(), &state_wrapper))
            };
            let result = compute_runtime.await_on(fut).await??;
            match result {
                BlockingSinkStatus::NeedMoreInput => {}
                BlockingSinkStatus::Finished => {
                    break;
                }
            }
        }

        // Take the state out of the Arc and Mutex because we need to return it.
        // It should be guaranteed that the ONLY holder of state at this point is this function.
        Ok(Arc::into_inner(state_wrapper)
            .expect("Completed worker should have exclusive access to state wrapper")
            .inner
            .into_inner()
            .expect("Completed worker should have exclusive access to inner state"))
    }

    fn spawn_workers(
        op: Arc<dyn BlockingSink>,
        input_receivers: Vec<Receiver<PipelineResultType>>,
        task_set: &mut TaskSet<DaftResult<Box<dyn DynBlockingSinkState>>>,
        stats: Arc<RuntimeStatsContext>,
    ) {
        for input_receiver in input_receivers {
            task_set.spawn(Self::run_worker(op.clone(), input_receiver, stats.clone()));
        }
    }

    // Forwards input from the child to the workers in a round-robin fashion.
    pub async fn forward_input_to_workers(
        mut receiver: CountingReceiver,
        worker_senders: Vec<Sender<PipelineResultType>>,
        morsel_size: Option<usize>,
    ) -> DaftResult<()> {
        let mut next_worker_idx = 0;
        let mut send_to_next_worker = |data: PipelineResultType| {
            let next_worker_sender = worker_senders.get(next_worker_idx).unwrap();
            next_worker_idx = (next_worker_idx + 1) % worker_senders.len();
            next_worker_sender.send(data)
        };

        let mut buffer = morsel_size.map(RowBasedBuffer::new);
        while let Some(morsel) = receiver.recv().await {
            if morsel.should_broadcast() {
                for worker_sender in &worker_senders {
                    let _ = worker_sender.send(morsel.clone()).await;
                }
            } else if let Some(buffer) = buffer.as_mut() {
                buffer.push(morsel.as_data().clone());
                if let Some(ready) = buffer.pop_enough()? {
                    for r in ready {
                        let _ = send_to_next_worker(r.into()).await;
                    }
                }
            } else {
                let _ = send_to_next_worker(morsel).await;
            }
        }
        if let Some(buffer) = buffer.as_mut() {
            // Clear all remaining morsels
            if let Some(last_morsel) = buffer.pop_all()? {
                let _ = send_to_next_worker(last_morsel.into()).await;
            }
        }
        Ok(())
    }
}

impl TreeDisplay for BlockingSinkNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        use common_display::DisplayLevel::Compact;
        if matches!(level, Compact) {
        } else {
            let rt_result = self.runtime_stats.result();
            rt_result.display(&mut display, true, true, true).unwrap();
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl PipelineNode for BlockingSinkNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let child = self.child.as_mut();
        let child_results_receiver = child
            .start(false, runtime_handle)?
            .get_receiver_with_stats(&self.runtime_stats);

        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);
        let op = self.op.clone();
        let runtime_stats = self.runtime_stats.clone();
        let num_workers = op.max_concurrency();
        let (input_senders, input_receivers) = (0..num_workers).map(|_| create_channel(1)).unzip();
        let morsel_size = runtime_handle.determine_morsel_size(op.morsel_size());
        runtime_handle.spawn(
            Self::forward_input_to_workers(child_results_receiver, input_senders, morsel_size),
            self.name(),
        );
        runtime_handle.spawn(
            async move {
                let mut task_set = TaskSet::new();
                Self::spawn_workers(
                    op.clone(),
                    input_receivers,
                    &mut task_set,
                    runtime_stats.clone(),
                );

                let mut finished_states = Vec::with_capacity(num_workers);
                while let Some(result) = task_set.join_next().await {
                    let state = result.context(JoinSnafu)??;
                    finished_states.push(state);
                }

                let compute_runtime = get_compute_runtime();
                let finalized_result = compute_runtime
                    .await_on(async move {
                        runtime_stats.in_span(&info_span!("BlockingSinkNode::finalize"), || {
                            op.finalize(finished_states)
                        })
                    })
                    .await??;
                if let Some(res) = finalized_result {
                    let _ = destination_sender.send(res).await;
                }
                Ok(())
            },
            self.name(),
        );
        Ok(destination_channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
