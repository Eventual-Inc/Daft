use std::{pin::Pin, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_micropartition::MicroPartition;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use lazy_static::lazy_static;
use snafu::ResultExt;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    create_channel,
    intermediate_ops::intermediate_op::{run_intermediate_operators, IntermediateOperator},
    sinks::sink::{launch_sink, Sink},
    sources::source::Source,
    Sender,
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
                let handle = tokio::spawn(run_intermediate_operators(
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

        let sink_handle = tokio::spawn(launch_sink(sink_receivers, sink, send_to_next_source));

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
