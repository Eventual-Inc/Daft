use std::{borrow::BorrowMut, pin::Pin, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_micropartition::MicroPartition;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use lazy_static::lazy_static;
use snafu::ResultExt;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    create_channel,
    intermediate_ops::intermediate_op::IntermediateOperator,
    sinks::sink::{Sink, SinkResultType},
    sources::source::Source,
    Receiver, Sender,
};

lazy_static! {
    static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

#[derive(Clone)]
pub struct Pipeline {
    source: Box<dyn Source>,
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
    sink: Option<Box<dyn Sink>>,
}

impl Pipeline {
    pub fn new(source: Box<dyn Source>) -> Self {
        Self {
            source,
            intermediate_operators: vec![],
            sink: None,
        }
    }

    pub fn with_intermediate_operator(mut self, op: Box<dyn IntermediateOperator>) -> Self {
        self.intermediate_operators.push(op);
        self
    }

    pub fn with_sink(mut self, sink: Box<dyn Sink>) -> Self {
        self.sink = Some(sink);
        self
    }

    async fn run_intermediate_operators(
        mut receiver: Receiver,
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        send_to_sink: Sender,
    ) -> DaftResult<()> {
        log::debug!(
            "Running intermediate operators: {}",
            intermediate_operators
                .iter()
                .fold(String::new(), |acc, op| { acc + &op.name() + " -> " })
        );

        while let Some(morsel) = receiver.recv().await {
            let mut data = morsel?;
            for op in intermediate_operators.iter() {
                data = op.execute(&data)?;
            }
            let _ = send_to_sink.send(Ok(data)).await;
        }

        log::debug!("Intermediate operators finished");
        Ok(())
    }

    async fn launch_sink(
        mut receivers: Vec<Receiver>,
        mut sink: Option<Box<dyn Sink>>,
        send_to_next_source: Sender,
    ) -> DaftResult<()> {
        let in_order = match sink {
            Some(ref sink) => sink.in_order(),
            None => false,
        };
        log::debug!("Launching sink with in_order: {}", in_order);

        if in_order {
            let mut finished_receiver_idxs = std::collections::HashSet::new();
            let mut curr_idx = 0;
            while finished_receiver_idxs.len() != receivers.len() {
                if finished_receiver_idxs.contains(&curr_idx) {
                    curr_idx = (curr_idx + 1) % receivers.len();
                    continue;
                }
                let receiver = receivers.get_mut(curr_idx).expect("Receiver not found");
                if let Some(val) = receiver.recv().await {
                    if let Some(sink) = sink.borrow_mut() {
                        let sink_result = sink.sink(&val?)?;
                        match sink_result {
                            SinkResultType::Finished => {
                                break;
                            }
                            SinkResultType::NeedMoreInput => {}
                        }
                    } else {
                        log::debug!("No sink, sending value to next source");
                        let _ = send_to_next_source.send(val).await;
                    }
                } else {
                    finished_receiver_idxs.insert(curr_idx);
                }
                curr_idx = (curr_idx + 1) % receivers.len();
            }
        } else {
            let mut unordered_receivers = FuturesUnordered::new();
            for receiver in receivers.iter_mut() {
                unordered_receivers.push(receiver.recv());
            }
            while let Some(val) = unordered_receivers.next().await {
                if let Some(val) = val {
                    if let Some(sink) = sink.borrow_mut() {
                        let sink_result = sink.sink(&val?)?;
                        match sink_result {
                            SinkResultType::Finished => {
                                break;
                            }
                            SinkResultType::NeedMoreInput => {}
                        }
                    } else {
                        log::debug!("No sink, sending value to next source");
                        let _ = send_to_next_source.send(val).await;
                    }
                }
            }
        }

        if let Some(sink) = sink.borrow_mut() {
            let final_values = sink.finalize()?;
            for value in final_values {
                log::debug!("Sending finalized value to next source");
                let _ = send_to_next_source.send(Ok(value)).await;
            }
        }
        Ok(())
    }

    pub async fn run(
        send_to_next_source: Sender,
        source: Box<dyn Source>,
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sink: Option<Box<dyn Sink>>,
    ) -> DaftResult<()> {
        log::debug!("Running pipeline");

        let source_stream = source.get_data();
        pin_mut!(source_stream);

        let mut intermediate_senders_and_handles: Vec<(Sender, JoinHandle<DaftResult<()>>)> =
            Vec::with_capacity(*NUM_CPUS);
        let mut sink_receivers = vec![];

        let mut curr_idx = 0;
        while let Some(morsel) = source_stream.next().await {
            if let Some((tx, _)) = intermediate_senders_and_handles.get_mut(curr_idx) {
                let _ = tx.send(Ok(morsel?)).await;
            } else {
                let (tx1, rx1) = create_channel();
                let (tx2, rx2) = create_channel();
                let handle = tokio::spawn(Self::run_intermediate_operators(
                    rx1,
                    intermediate_operators.clone(),
                    tx2,
                ));
                intermediate_senders_and_handles.push((tx1.clone(), handle));
                sink_receivers.push(rx2);
                let _ = tx1.send(Ok(morsel?)).await;
            }

            curr_idx = (curr_idx + 1) % intermediate_senders_and_handles.len();
        }

        let sink_handle =
            tokio::spawn(Self::launch_sink(sink_receivers, sink, send_to_next_source));

        let mut handles_unordered = FuturesUnordered::new();
        for (sender, handle) in intermediate_senders_and_handles {
            drop(sender);
            handles_unordered.push(handle);
        }
        handles_unordered.push(sink_handle);

        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                log::debug!("Ctrl-c cancelled execution");
                return Err(DaftError::InternalError(
                    "Ctrl-c cancelled execution.".to_string(),
                ));
            }
            Some(result) = handles_unordered.next() => {
                result.context(crate::JoinSnafu {})??;
            }
        }
        log::debug!("Pipeline finished");
        Ok(())
    }
}

impl Source for Pipeline {
    fn get_data(&self) -> Pin<Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
        log::debug!("Pipeline::get_data");
        let (tx, rx) = create_channel();
        let source = self.source.clone();
        let intermediate_operators = self.intermediate_operators.clone();
        let sink = self.sink.clone();

        tokio::spawn(async move {
            let pipeline_result = Self::run(tx.clone(), source, intermediate_operators, sink).await;
            if let Err(e) = pipeline_result {
                let _ = tx.send(Err(e)).await;
            }
        });

        Box::pin(ReceiverStream::new(rx))
    }
}
