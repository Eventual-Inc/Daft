use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};
use snafu::ResultExt;

use crate::{
    channel::{
        create_channel, create_single_channel, MultiReceiver, MultiSender, SingleReceiver,
        SingleSender,
    },
    JoinSnafu, TaskSet, NUM_CPUS,
};

use super::state::SinkTaskState;

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

pub trait SingleInputSink: Send + Sync {
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType>;
    fn in_order(&self) -> bool;
    fn can_parallelize(&self) -> bool;
    fn finalize(&self, input: &Arc<MicroPartition>) -> DaftResult<Vec<Arc<MicroPartition>>>;
}

// A SingleInputSinkRunner runs a single input sink in parallel.
// It receives data, spawns parallel tasks to process the data via the sink method,
// collects the sinked data, finalizes it, and sends the results.
pub struct SingleInputSinkRunner {
    sink: Arc<dyn SingleInputSink>,
    receiver: MultiReceiver,
    sender: MultiSender,
}

impl SingleInputSinkRunner {
    pub fn new(
        sink: Arc<dyn SingleInputSink>,
        receiver: MultiReceiver,
        sender: MultiSender,
    ) -> Self {
        Self {
            sink,
            receiver,
            sender,
        }
    }

    #[instrument(level = "info", skip(self), name = "SingleInputSinkActor::run")]
    // Run a single instance of the sink.
    pub async fn run_single_sinker(
        mut receiver: SingleReceiver,
        sink: Arc<dyn SingleInputSink>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let mut state = SinkTaskState::new();
        while let Some(morsel) = receiver.recv().await {
            let _sink_span = info_span!("Sink::sink").entered();

            let result = sink.sink(&morsel, &mut state)?;
            match result {
                SinkResultType::NeedMoreInput => {
                    continue;
                }
                SinkResultType::Finished => {
                    break;
                }
            }
        }
        state.clear()
    }

    // Create and run parallel tasks for the sink.
    pub async fn run_parallel(&mut self) -> DaftResult<()> {
        // Initialize a task set and sendesr to send data to parallel tasks.
        let mut inner_task_set = TaskSet::<Option<Arc<MicroPartition>>>::new();
        let mut inner_task_senders: Vec<SingleSender> =
            Vec::with_capacity(self.sink.can_parallelize().then(|| *NUM_CPUS).unwrap_or(1));
        let mut curr_task_idx = 0;

        // Receive data and send it to parallel tasks.
        while let Some(val) = self.receiver.recv().await {
            // Send the data to a parallel task if the sender exists.
            if let Some(sender) = inner_task_senders.get(curr_task_idx) {
                let _ = sender.send(val).await;
            }
            // Otherwise, create a new parallel task and send the data to it.
            else {
                let (sender, receiver) = create_single_channel(1);
                inner_task_senders.push(sender.clone());
                let sink = self.sink.clone();
                inner_task_set.spawn(async move { Self::run_single_sinker(receiver, sink).await });
                let _ = sender.send(val).await;
            }
            curr_task_idx = (curr_task_idx + 1) % inner_task_senders.len();
        }
        // Wait for the parallel tasks to finish and collect the results.
        drop(inner_task_senders);

        // Collect the results from the parallel task.
        let mut buffer = Vec::new();
        while let Some(result) = inner_task_set.join_next().await {
            let val = result.context(JoinSnafu {})??;
            if let Some(part) = val {
                buffer.push(part);
            }
        }

        // Finalize the results and send them.
        if !buffer.is_empty() {
            let concated =
                MicroPartition::concat(&buffer.iter().map(|p| p.as_ref()).collect::<Vec<_>>())?;
            let finalized = self.sink.finalize(&Arc::new(concated))?;
            for val in finalized {
                let _ = self.sender.get_next_sender().send(val).await;
            }
        }
        Ok(())
    }
}

pub fn run_single_input_sink_and_get_next_sender(
    sink: Arc<dyn SingleInputSink>,
    send_to: MultiSender,
    op_set: &mut TaskSet<()>,
) -> MultiSender {
    let (sender, receiver) = create_channel(*NUM_CPUS, sink.in_order());
    let mut runner = SingleInputSinkRunner::new(sink, receiver, send_to);
    op_set.spawn(async move { runner.run_parallel().await });
    sender
}

pub trait DoubleInputSink: Send + Sync {
    fn sink_left(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType>;
    fn sink_right(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType>;
    fn in_order(&self) -> bool;
    fn finalize(
        &self,
        input_left: &Arc<MicroPartition>,
        input_right: &Arc<MicroPartition>,
    ) -> DaftResult<Vec<Arc<MicroPartition>>>;
    fn name(&self) -> &'static str;
}

// A DoubleInputSinkRunner runs a double input sink in parallel.
// It receives data, spawns parallel tasks to process the data via the sink_left and sink_right methods,
// collects the sinked data, finalizes it, and sends the results.
pub struct DoubleInputSinkRunner {
    sink: Arc<dyn DoubleInputSink>,
    left_receiver: MultiReceiver,
    right_receiver: MultiReceiver,
    sender: MultiSender,
}

impl DoubleInputSinkRunner {
    pub fn new(
        sink: Arc<dyn DoubleInputSink>,
        left_receiver: MultiReceiver,
        right_receiver: MultiReceiver,
        sender: MultiSender,
    ) -> Self {
        Self {
            sink,
            left_receiver,
            right_receiver,
            sender,
        }
    }

    #[instrument(level = "info", skip(self), name = "DoubleInputSinkActor::run")]
    // TODO: Implement parallelization for double input sink.
    pub async fn run(&mut self) -> DaftResult<()> {
        log::debug!("Running DoubleInputSinkRunner for : {}", self.sink.name());
        let mut left_state = SinkTaskState::new();
        while let Some(val) = self.left_receiver.recv().await {
            let _sink_span = info_span!("Sink::sink").entered();
            let sink_result = self.sink.sink_left(&val, &mut left_state)?;
            match sink_result {
                SinkResultType::NeedMoreInput => {
                    continue;
                }
                SinkResultType::Finished => {
                    break;
                }
            }
        }

        let mut right_state = SinkTaskState::new();
        while let Some(val) = self.right_receiver.recv().await {
            let _sink_span = info_span!("Sink::sink").entered();
            let sink_result = self.sink.sink_right(&val, &mut right_state)?;
            match sink_result {
                SinkResultType::NeedMoreInput => {
                    continue;
                }
                SinkResultType::Finished => {
                    break;
                }
            }
        }

        let final_span = info_span!("Sink::finalize");

        let left_part = left_state.clear()?;
        if let Some(left_part) = left_part {
            let right_part = right_state.clear()?;
            if let Some(right_part) = right_part {
                let finalized = final_span.in_scope(|| self.sink.finalize()&left_part, &right_part)?;
                for val in finalized {
                    let _ = self.sender.get_next_sender().send(val).await;
                }
            }
        }
        Ok(())
    }
}

pub fn run_double_input_sink_and_get_next_sender(
    sink: Arc<dyn DoubleInputSink>,
    send_to: MultiSender,
    op_set: &mut TaskSet<()>,
) -> (MultiSender, MultiSender) {
    let (left_sender, left_receiver) = create_channel(*NUM_CPUS, sink.in_order());
    let (right_sender, right_receiver) = create_channel(*NUM_CPUS, sink.in_order());
    let mut runner = DoubleInputSinkRunner::new(sink, left_receiver, right_receiver, send_to);
    op_set.spawn(async move { runner.run().await });
    (left_sender, right_sender)
}
