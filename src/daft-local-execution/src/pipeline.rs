use std::{borrow::BorrowMut, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
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
    JoinSnafu, Receiver, Sender,
};

lazy_static! {
    pub static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
}

pub struct InnerPipelineManager {
    senders: Vec<Sender>,
    curr_idx: usize,
    handles: Vec<JoinHandle<DaftResult<()>>>,
}

impl InnerPipelineManager {
    pub fn new(
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sink_senders: Vec<Sender>,
    ) -> Self {
        let mut senders = vec![];
        let mut handles = vec![];

        for sink_sender in sink_senders.into_iter() {
            let (source_sender, source_receiver) = create_channel();
            let handle = tokio::spawn(Self::run_single_inner_pipeline(
                intermediate_operators.clone(),
                sink_sender,
                source_receiver,
            ));

            handles.push(handle);
            senders.push(source_sender);
        }
        Self {
            senders,
            curr_idx: 0,
            handles,
        }
    }

    async fn run_single_inner_pipeline(
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sender: Sender,
        mut receiver: Receiver,
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
            let _ = sender.send(Ok(result)).await;
        }

        log::debug!("Intermediate operators finished");
        Ok(())
    }

    pub fn get_next_sender(&mut self) -> Sender {
        let idx = self.curr_idx;
        self.curr_idx = (self.curr_idx + 1) % self.senders.len();
        self.senders[idx].clone()
    }

    pub fn retrieve_handles(self) -> Vec<JoinHandle<DaftResult<()>>> {
        self.handles
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

    pub async fn run(&self, send_to_next_source: Sender) -> DaftResult<()> {
        log::debug!("Running pipeline");

        let mut all_handles = vec![];

        // Initialize the channels to send morsels from the source to the inner pipelines, and from the inner pipelines to the sink
        let (sink_senders, sink_receivers) = (0..*NUM_CPUS)
            .map(|_| create_channel())
            .unzip::<_, _, Vec<_>, Vec<_>>();

        // Spawn the sink manager
        let sink = self.sink.clone();
        let sink_job = tokio::spawn(async move {
            let mut sink_manager = SinkManager::new(sink, send_to_next_source);
            sink_manager.run(sink_receivers).await
        });
        all_handles.push(sink_job);

        // Create the inner pipeline manager and send the data from the source to the inner pipelines
        let mut inner_pipeline_manager =
            InnerPipelineManager::new(self.intermediate_operators.clone(), sink_senders);

        // Get the data from the source
        let source_stream = self.source.get_data().await;
        pin_mut!(source_stream);
        while let Some(morsel) = source_stream.next().await {
            let inner_pipeline_sender = inner_pipeline_manager.get_next_sender();
            let _ = inner_pipeline_sender.send(morsel).await;
        }
        let inner_pipeline_jobs = inner_pipeline_manager.retrieve_handles();
        all_handles.extend(inner_pipeline_jobs);

        // Wait for all the tasks to finish
        let awaiting_all_handles = async {
            for handle in all_handles.iter_mut() {
                let _ = handle.await.context(JoinSnafu {});
            }
            Ok(())
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                for handle in all_handles {
                    handle.abort();
                }
            }
            result = awaiting_all_handles => {
                result.context(JoinSnafu {})?;
            }
        }

        log::debug!("Pipeline finished");
        Ok(())
    }
}

#[async_trait]
impl Source for Pipeline {
    async fn get_data(
        &self,
    ) -> Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send + Unpin> {
        log::debug!("Pipeline::get_data");
        let (tx, rx) = create_channel();

        let _ = self.run(tx.clone()).await;

        Box::new(ReceiverStream::new(rx))
    }
}
