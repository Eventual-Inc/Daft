use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use lazy_static::lazy_static;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    create_channel,
    intermediate_ops::intermediate_op::IntermediateOperator,
    sinks::sink::{Sink, SinkResultType},
    sources::source::{Source, SourceStream},
    Receiver, Sender,
};

lazy_static! {
    pub static ref NUM_CPUS: usize = 1;
}

pub struct InnerPipelineManager {
    senders: Vec<Sender>,
    curr_idx: usize,
}

impl InnerPipelineManager {
    pub fn new(
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sink_senders: Vec<Sender>,
    ) -> Self {
        let mut senders = vec![];

        for sink_sender in sink_senders.into_iter() {
            let (source_sender, source_receiver) = create_channel();
            tokio::spawn(Self::run_single_inner_pipeline(
                intermediate_operators.clone(),
                sink_sender,
                source_receiver,
            ));

            senders.push(source_sender);
        }
        Self {
            senders,
            curr_idx: 0,
        }
    }

    fn execute_intermediate_operators(
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        morsel: DaftResult<Arc<MicroPartition>>,
    ) -> DaftResult<Arc<MicroPartition>> {
        let mut result = morsel;
        for op in intermediate_operators.iter() {
            result = op.execute(&result?);
        }
        result
    }

    async fn run_single_inner_pipeline(
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sender: Sender,
        mut receiver: Receiver,
    ) -> DaftResult<()> {
        while let Some(morsel) = receiver.recv().await {
            let result =
                Self::execute_intermediate_operators(intermediate_operators.clone(), morsel);
            let _ = sender.send(result).await;
        }
        Ok(())
    }

    pub fn get_next_sender(&mut self) -> Sender {
        let idx = self.curr_idx;
        self.curr_idx = (self.curr_idx + 1) % self.senders.len();
        self.senders[idx].clone()
    }
}

pub struct SinkManager {
    sink: Arc<Mutex<dyn Sink>>,
}

impl SinkManager {
    pub fn new(sink: Arc<Mutex<dyn Sink>>) -> Self {
        Self { sink }
    }

    async fn receive_until_finished(
        mut receiver: Receiver,
        sink: Arc<Mutex<dyn Sink>>,
    ) -> DaftResult<SinkResultType> {
        while let Some(val) = receiver.recv().await {
            let sink_result = sink.lock().await.sink(&val?)?;
            match sink_result {
                SinkResultType::NeedMoreInput => {
                    continue;
                }
                SinkResultType::Finished => {
                    return Ok(SinkResultType::Finished);
                }
            }
        }
        Ok(SinkResultType::NeedMoreInput)
    }

    pub async fn run(
        &mut self,
        receivers: Vec<Receiver>,
        in_order: bool,
        sender: Sender,
    ) -> DaftResult<()> {
        if in_order {
            let mut in_order_receivers = receivers
                .into_iter()
                .map(|rx| Self::receive_until_finished(rx, self.sink.clone()))
                .collect::<FuturesOrdered<_>>();
            while let Some(result) = in_order_receivers.next().await {
                if matches!(result?, SinkResultType::Finished) {
                    break;
                }
            }
        } else {
            let mut unordered_receivers = receivers
                .into_iter()
                .map(|rx| Self::receive_until_finished(rx, self.sink.clone()))
                .collect::<FuturesUnordered<_>>();
            while let Some(result) = unordered_receivers.next().await {
                if matches!(result?, SinkResultType::Finished) {
                    break;
                }
            }
        }

        let finalized_results = self.sink.lock().await.finalize()?;
        for part in finalized_results {
            let _ = sender.send(Ok(part)).await;
        }
        Ok(())
    }
}

pub struct Pipeline {
    source: Arc<dyn Source>,
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
    sink: Option<Arc<Mutex<dyn Sink>>>,
    in_order: bool,
}

impl Pipeline {
    pub fn new(source: Arc<dyn Source>) -> Self {
        Self {
            source,
            intermediate_operators: vec![],
            sink: None,
            in_order: false,
        }
    }

    pub fn with_intermediate_operator(mut self, op: Box<dyn IntermediateOperator>) -> Self {
        self.intermediate_operators.push(op);
        self
    }

    pub fn with_sink(mut self, sink: Arc<Mutex<dyn Sink>>, in_order: bool) -> Self {
        self.sink = Some(sink);
        self.in_order = in_order;
        self
    }

    fn launch_source_forwarder(source: Arc<dyn Source>, sender: Sender) {
        tokio::spawn(async move {
            let mut source_stream = source.get_data();
            while let Some(morsel) = source_stream.next().await {
                let _ = sender.send(morsel).await;
            }
        });
    }

    fn launch_inner_pipelines(
        source: Arc<dyn Source>,
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        sink_senders: Vec<Sender>,
    ) {
        tokio::spawn(async move {
            let mut source_stream = source.get_data();
            let mut inner_pipeline_manager =
                InnerPipelineManager::new(intermediate_operators, sink_senders);
            while let Some(morsel) = source_stream.next().await {
                let inner_pipeline_sender = inner_pipeline_manager.get_next_sender();
                let _ = inner_pipeline_sender.send(morsel).await;
            }
        });
    }

    fn launch_sink(
        sink: Arc<Mutex<dyn Sink>>,
        in_order: bool,
        stream_sender: Sender,
        sink_receivers: Vec<Receiver>,
    ) {
        tokio::spawn(async move {
            let mut sink_manager = SinkManager::new(sink);
            sink_manager
                .run(sink_receivers, in_order, stream_sender)
                .await
        });
    }

    pub fn run(&self) -> SourceStream {
        let (stream_sender, stream_receiver) = create_channel();

        match (&self.sink, self.intermediate_operators.len()) {
            (None, 0) => {
                Self::launch_source_forwarder(self.source.clone(), stream_sender);
            }
            (None, _) => {
                Self::launch_inner_pipelines(
                    self.source.clone(),
                    self.intermediate_operators.clone(),
                    (0..*NUM_CPUS).map(|_| stream_sender.clone()).collect(),
                );
            }
            (Some(sink), 0) => {
                let (sink_sender, sink_receiver) = create_channel();

                let in_order = self.in_order;
                Self::launch_sink(sink.clone(), in_order, stream_sender, vec![sink_receiver]);

                Self::launch_source_forwarder(self.source.clone(), sink_sender);
            }
            (Some(sink), _) => {
                let (sink_senders, sink_receivers) = (0..*NUM_CPUS)
                    .map(|_| create_channel())
                    .unzip::<_, _, Vec<_>, Vec<_>>();

                let in_order = self.in_order;
                Self::launch_sink(sink.clone(), in_order, stream_sender, sink_receivers);

                Self::launch_inner_pipelines(
                    self.source.clone(),
                    self.intermediate_operators.clone(),
                    sink_senders,
                );
            }
        }
        ReceiverStream::new(stream_receiver).boxed()
    }
}

impl Source for Pipeline {
    fn get_data(&self) -> SourceStream {
        self.run()
    }
}
