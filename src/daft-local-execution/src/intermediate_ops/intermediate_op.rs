use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    channel::{
        create_channel, create_single_channel, MultiReceiver, MultiSender, SingleReceiver,
        SingleSender,
    },
    TaskSet, NUM_CPUS,
};

use super::state::OperatorTaskState;

pub trait IntermediateOperator: Send + Sync {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>>;
}

/// An IntermediateOpRunner runs an intermediate operator in parallel.
/// It receives data, spawns parallel tasks to process the data, and sends the results.
pub struct IntermediateOpRunner {
    sender: MultiSender,
    receiver: MultiReceiver,
    op: Arc<dyn IntermediateOperator>,
}

impl IntermediateOpRunner {
    pub fn new(
        op: Arc<dyn IntermediateOperator>,
        receiver: MultiReceiver,
        sender: MultiSender,
    ) -> Self {
        Self {
            op,
            receiver,
            sender,
        }
    }

    // Run a single instance of the operator.
    #[instrument(level = "info", skip_all, name = "IntermediateOpActor::run_single")]
    async fn run_single(
        mut receiver: SingleReceiver,
        sender: SingleSender,
        op: Arc<dyn IntermediateOperator>,
    ) -> DaftResult<()> {
        let mut state = OperatorTaskState::default();
        let span = info_span!("IntermediateOp::execute");

        while let Some(morsel) = receiver.recv().await {
            let result = span.in_scope(|| op.execute(&morsel))?;

            state.push(result);
            if let Some(part) = state.try_clear() {
                let _ = sender.send(part?).await;
            }
        }
        if let Some(part) = state.clear() {
            let _ = sender.send(part?).await;
        }
        Ok(())
    }

    // Create and run parallel tasks for the operator.
    #[instrument(level = "info", skip_all, name = "IntermediateOpActor::run_parallel")]
    pub async fn run_parallel(&mut self) -> DaftResult<()> {
        // Initialize senders to send data to parallel tasks.
        let mut inner_task_senders: Vec<SingleSender> =
            Vec::with_capacity(self.sender.buffer_size());
        let mut inner_task_set = TaskSet::new();
        let mut curr_task_idx = 0;

        while let Some(morsel) = self.receiver.recv().await {
            // If the task sender already exists for the current index, send the data to it.
            if let Some(s) = inner_task_senders.get(curr_task_idx) {
                let _ = s.send(morsel).await;
            }
            // Otherwise, create a new task and send the data to it.
            else {
                let (task_sender, task_receiver) = create_single_channel(1);
                let next_sender = self.sender.get_next_sender();
                inner_task_set.spawn(Self::run_single(
                    task_receiver,
                    next_sender,
                    self.op.clone(),
                ));
                let _ = task_sender.send(morsel).await;

                inner_task_senders.push(task_sender);
            }
            curr_task_idx = (curr_task_idx + 1) % self.sender.buffer_size();
        }

        // Drop the senders to signal that no more data will be sent.
        drop(inner_task_senders);

        // Wait for all tasks to finish and propagate any errors.
        inner_task_set.join_all().await?;
        Ok(())
    }
}

pub fn run_intermediate_op_and_get_next_sender(
    op: Arc<dyn IntermediateOperator>,
    send_to: MultiSender,
    op_set: &mut TaskSet<()>,
) -> MultiSender {
    let (sender, receiver) = create_channel(*NUM_CPUS, send_to.in_order());
    let mut runner = IntermediateOpRunner::new(op, receiver, send_to);
    op_set.spawn(async move { runner.run_parallel().await });
    sender
}
