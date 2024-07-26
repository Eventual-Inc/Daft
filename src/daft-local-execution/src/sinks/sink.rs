use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, MultiReceiver, MultiSender},
    NUM_CPUS,
};

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

pub trait SingleInputSink: Send + Sync {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn in_order(&self) -> bool;
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>>;
}

pub struct SingleInputSinkActor {
    sink: Box<dyn SingleInputSink>,
    receiver: MultiReceiver,
    sender: MultiSender,
}

impl SingleInputSinkActor {
    pub fn new(
        sink: Box<dyn SingleInputSink>,
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
    pub async fn run(&mut self) -> DaftResult<()> {
        while let Some(val) = self.receiver.recv().await {
            let _sink_span = info_span!("Sink::sink").entered();

            let sink_result = self.sink.sink(&val?)?;
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

        let finalized_values = final_span.in_scope(|| self.sink.finalize())?;
        for val in finalized_values {
            let _ = self.sender.get_next_sender().send(Ok(val)).await;
        }
        Ok(())
    }
}

pub fn run_single_input_sink(sink: Box<dyn SingleInputSink>, send_to: MultiSender) -> MultiSender {
    let (sender, receiver) = create_channel(*NUM_CPUS, sink.in_order());
    let mut actor = SingleInputSinkActor::new(sink, receiver, send_to);
    tokio::spawn(async move {
        let _ = actor.run().await;
    });
    sender
}

pub trait DoubleInputSink: Send + Sync + dyn_clone::DynClone {
    fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn in_order(&self) -> bool;
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>>;
    fn name(&self) -> &'static str;
}

dyn_clone::clone_trait_object!(DoubleInputSink);

pub struct DoubleInputSinkActor {
    sink: Box<dyn DoubleInputSink>,
    left_receiver: MultiReceiver,
    right_receiver: MultiReceiver,
    sender: MultiSender,
}

impl DoubleInputSinkActor {
    pub fn new(
        sink: Box<dyn DoubleInputSink>,
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
    pub async fn run(&mut self) -> DaftResult<()> {
        while let Some(val) = self.left_receiver.recv().await {
            let _sink_span = info_span!("Sink::sink").entered();
            let sink_result = self.sink.sink_left(&val?)?;
            match sink_result {
                SinkResultType::NeedMoreInput => {
                    continue;
                }
                SinkResultType::Finished => {
                    break;
                }
            }
        }

        while let Some(val) = self.right_receiver.recv().await {
            let _sink_span = info_span!("Sink::sink").entered();
            let sink_result = self.sink.sink_right(&val?)?;
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

        let finalized_values = final_span.in_scope(|| self.sink.finalize())?;
        for val in finalized_values {
            let _ = self.sender.get_next_sender().send(Ok(val)).await;
        }
        Ok(())
    }
}

pub fn run_double_input_sink(
    sink: Box<dyn DoubleInputSink>,
    send_to: MultiSender,
) -> (MultiSender, MultiSender) {
    let (left_sender, left_receiver) = create_channel(*NUM_CPUS, sink.in_order());
    let (right_sender, right_receiver) = create_channel(*NUM_CPUS, sink.in_order());
    let mut actor = DoubleInputSinkActor::new(sink, left_receiver, right_receiver, send_to);
    tokio::spawn(async move {
        let _ = actor.run().await;
    });
    (left_sender, right_sender)
}
