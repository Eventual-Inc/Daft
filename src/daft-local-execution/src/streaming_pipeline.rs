use std::{borrow::BorrowMut, pin::Pin, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_micropartition::MicroPartition;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use snafu::ResultExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    create_channel,
    intermediate_ops::intermediate_op::IntermediateOperator,
    sinks::sink::{Sink, SinkResultType},
    sources::source::Source,
    Receiver, Sender,
};

#[derive(Clone)]
pub struct StreamingPipeline {
    source: Box<dyn Source>,
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
    sink: Option<Box<dyn Sink>>,
}

impl StreamingPipeline {
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
        morsel: Arc<MicroPartition>,
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
        send_to_sink: Sender,
    ) -> DaftResult<()> {
        println!(
            "Running intermediate operators: {}",
            intermediate_operators.len()
        );

        let mut data = morsel;
        for op in intermediate_operators {
            data = op.execute(&data)?;
        }
        let _ = send_to_sink.send(Ok(data)).await;
        println!("Intermediate operators finished");
        Ok(())
    }

    async fn launch_sink(
        mut receivers: Vec<Receiver>,
        mut sink: Option<Box<dyn Sink>>,
        send_to_next_source: Sender,
    ) -> DaftResult<()> {
        println!("Launching sink");
        let in_order = match sink {
            Some(ref sink) => sink.in_order(),
            None => false,
        };

        if in_order {
            for receiver in receivers.iter_mut() {
                while let Some(val) = receiver.recv().await {
                    if let Some(sink) = sink.borrow_mut() {
                        let sink_result = sink.sink(&val?)?;
                        match sink_result {
                            SinkResultType::Finished => {
                                break;
                            }
                            SinkResultType::NeedMoreInput => {}
                        }
                    } else {
                        let _ = send_to_next_source.send(val).await;
                    }
                }
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
                        let _ = send_to_next_source.send(val).await;
                    }
                }
            }
        }

        if let Some(sink) = sink.borrow_mut() {
            println!("Finalizing sink");
            let final_values = sink.finalize()?;
            for value in final_values {
                println!("Sending finalized value back to pipeline");
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
        println!("Running pipeline");

        let source_stream = source.get_data();
        pin_mut!(source_stream);

        let mut handles = FuturesUnordered::new();
        let mut receivers = vec![];
        while let Some(morsel) = source_stream.next().await {
            let morsel = morsel?;
            let (tx, rx) = create_channel();
            let intermediate_operators = intermediate_operators.clone();
            let handle = tokio::spawn(Self::run_intermediate_operators(
                morsel,
                intermediate_operators,
                tx,
            ));
            handles.push(handle);
            receivers.push(rx);
        }

        let sink_handle = tokio::spawn(Self::launch_sink(receivers, sink, send_to_next_source));
        handles.push(sink_handle);

        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                println!("Ctrl-c cancelled execution.");
                return Err(DaftError::InternalError(
                    "Ctrl-c cancelled execution.".to_string(),
                ));
            }
            Some(result) = handles.next() => {
                let _ = result.context(crate::JoinSnafu {})??;
            }
        }
        println!("Pipeline finished");
        Ok(())
    }
}

impl Source for StreamingPipeline {
    fn get_data(&self) -> Pin<Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
        println!("StreamingPipeline::get_data");
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
