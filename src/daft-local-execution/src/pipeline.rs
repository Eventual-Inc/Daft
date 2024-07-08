use std::{borrow::BorrowMut, pin::Pin, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_micropartition::MicroPartition;
use futures::{
    pin_mut,
    stream::{FuturesOrdered, FuturesUnordered},
    Stream, StreamExt,
};
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

pub struct InnerPipelineManager {
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
    senders_and_handles: Vec<(Sender, JoinHandle<DaftResult<()>>)>,
    sink_receivers: Vec<Receiver>,
    curr_idx: usize,
}

impl InnerPipelineManager {
    pub fn new(intermediate_operators: Vec<Box<dyn IntermediateOperator>>) -> Self {
        Self {
            intermediate_operators,
            senders_and_handles: Vec::with_capacity(*NUM_CPUS),
            sink_receivers: vec![],
            curr_idx: 0,
        }
    }

    async fn run_single_inner_pipeline(
        mut receiver: Receiver,
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sink_sender: Sender,
    ) -> DaftResult<()> {
        log::debug!(
            "Running intermediate operators: {}",
            intermediate_operators
                .iter()
                .fold(String::new(), |acc, op| { acc + &op.name() + " -> " })
        );

        while let Some(morsel) = receiver.recv().await {
            let mut result = morsel?;
            for op in intermediate_operators.iter() {
                result = op.execute(&result)?;
            }
            let _ = sink_sender.send(Ok(result)).await;
        }

        log::debug!("Intermediate operators finished");
        Ok(())
    }

    fn create_next_sender(&mut self) -> Sender {
        let (intermediate_sender, intermediate_receiver) = create_channel();
        let (sink_sender, sink_receiver) = create_channel();
        let handle = tokio::spawn(Self::run_single_inner_pipeline(
            intermediate_receiver,
            self.intermediate_operators.clone(),
            sink_sender,
        ));

        self.sink_receivers.push(sink_receiver);
        self.senders_and_handles
            .push((intermediate_sender.clone(), handle));
        intermediate_sender
    }

    pub fn get_or_create_next_sender(&mut self) -> Sender {
        let sender = if let Some((tx, _)) = self.senders_and_handles.get(self.curr_idx) {
            tx.clone()
        } else {
            self.create_next_sender()
        };
        self.curr_idx = (self.curr_idx + 1) % self.senders_and_handles.len();
        sender
    }

    pub fn retrieve_handles(&mut self) -> Vec<JoinHandle<DaftResult<()>>> {
        self.senders_and_handles
            .drain(..)
            .map(|(_, handle)| handle)
            .collect()
    }
}

pub struct SinkManager {
    sink: Option<Box<dyn Sink>>,
    send_to_next_source: Sender,
}

impl SinkManager {
    pub fn new(sink: Option<Box<dyn Sink>>, send_to_next_source: Sender) -> Self {
        Self {
            sink,
            send_to_next_source,
        }
    }

    async fn process_value(
        &mut self,
        val: DaftResult<Arc<MicroPartition>>,
    ) -> DaftResult<SinkResultType> {
        if let Some(sink) = self.sink.borrow_mut() {
            sink.sink(&val?)
        } else {
            let _ = self.send_to_next_source.send(Ok(val?)).await;
            Ok(SinkResultType::NeedMoreInput)
        }
    }

    async fn finalize_values(&mut self) -> DaftResult<()> {
        if let Some(sink) = self.sink.borrow_mut() {
            for part in sink.finalize()? {
                let _ = self.send_to_next_source.send(Ok(part)).await;
            }
        }
        Ok(())
    }

    pub async fn run(&mut self, mut receivers: Vec<Receiver>) -> DaftResult<()> {
        let in_order = match self.sink {
            Some(ref sink) => sink.in_order(),
            None => false,
        };
        log::debug!("Launching sink with in_order: {}", in_order);

        if in_order {
            let mut in_order_receivers = receivers
                .iter_mut()
                .map(|r| r.recv())
                .collect::<FuturesOrdered<_>>();
            while let Some(val) = in_order_receivers.next().await {
                if let Some(val) = val {
                    if matches!(self.process_value(val).await?, SinkResultType::Finished) {
                        break;
                    }
                }
            }
        } else {
            let mut unordered_receivers = receivers
                .iter_mut()
                .map(|r| r.recv())
                .collect::<FuturesUnordered<_>>();
            while let Some(val) = unordered_receivers.next().await {
                if let Some(val) = val {
                    if matches!(self.process_value(val).await?, SinkResultType::Finished) {
                        break;
                    }
                }
            }
        }

        self.finalize_values().await
    }
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

    pub async fn run(
        send_to_next_source: Sender,
        source: Box<dyn Source>,
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sink: Option<Box<dyn Sink>>,
    ) -> DaftResult<()> {
        log::debug!("Running pipeline");

        // Initialize the unordered handles to store the handles of the inner pipelines and the sink
        let mut handles_unordered = FuturesUnordered::new();

        // Get the data from the source
        let source_stream = source.get_data();
        pin_mut!(source_stream);

        // Create the inner pipeline manager and send the data from the source to the inner pipelines
        let mut inner_pipeline_manager = InnerPipelineManager::new(intermediate_operators);
        while let Some(morsel) = source_stream.next().await {
            let inner_pipeline_sender = inner_pipeline_manager.get_or_create_next_sender();
            let _ = inner_pipeline_sender.send(morsel).await;
        }
        handles_unordered.extend(inner_pipeline_manager.retrieve_handles());

        // Create the sink manager and run the sink
        let sink_handle = tokio::spawn(async move {
            let mut sink_manager = SinkManager::new(sink, send_to_next_source);
            sink_manager
                .run(inner_pipeline_manager.sink_receivers)
                .await
        });
        handles_unordered.push(sink_handle);

        // Wait for the inner pipelines and the sink to finish
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
