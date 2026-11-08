//! Bounded streaming input for externally-driven native execution.
//!
//! Every logical source has an independent bounded queue.  Producers use
//! `try_*` operations so PostgreSQL's executor thread never waits inside
//! Daft.  A successful `Finish` is recorded exactly once; a full queue leaves
//! the source open so the same finish operation can be retried.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_local_plan::InputId;
use daft_micropartition::MicroPartitionRef;
use futures::{StreamExt, stream};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::source::{Source, SourceStream, StatsProvider};
use crate::pipeline::{NodeName, PipelineMessage};

/// Message delivered to one streaming source queue.
#[derive(Debug)]
pub enum StreamingMessage {
    /// A data batch belonging to one executor invocation.
    Batch {
        input_id: InputId,
        batch: MicroPartitionRef,
    },
    /// End of this source for one executor invocation.
    Finish { input_id: InputId },
}

/// Result of a non-blocking producer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingPushResult {
    Ok,
    Again,
    End,
    Error,
}

/// An unsuccessful ownership-preserving batch push.
#[derive(Debug)]
pub enum StreamingPushError {
    /// Queue is full.  The exact batch is returned for retry.
    Again(MicroPartitionRef),
    /// Source is finished or cancelled.  The batch remains caller-owned.
    End(MicroPartitionRef),
    /// The source index is invalid.  The batch remains caller-owned.
    InvalidSource(MicroPartitionRef),
}

/// Result of reserving queue capacity before constructing an owned batch.
///
/// The producer callback is invoked only after an mpsc permit has been
/// reserved.  This is the transaction boundary needed by foreign-memory
/// importers: `Again`, `End`, and `InvalidSource` never invoke the callback.
#[derive(Debug)]
pub enum StreamingBuildPushError<E> {
    /// The bounded source queue has no capacity.
    Again,
    /// The source is finished or cancelled.
    End,
    /// The source index does not exist in this sender.
    InvalidSource,
    /// Batch construction/import failed before the queue accepted a value.
    Build(E),
}

impl StreamingPushError {
    pub fn status(&self) -> StreamingPushResult {
        match self {
            Self::Again(_) => StreamingPushResult::Again,
            Self::End(_) => StreamingPushResult::End,
            Self::InvalidSource(_) => StreamingPushResult::Error,
        }
    }

    pub fn into_batch(self) -> MicroPartitionRef {
        match self {
            Self::Again(batch) | Self::End(batch) | Self::InvalidSource(batch) => batch,
        }
    }
}

#[derive(Debug)]
struct SourceEndpoint {
    sender: mpsc::Sender<StreamingMessage>,
    /// Serializes Batch/Finish against the transition to finished.
    finished: Mutex<bool>,
}

/// Cloneable producer side for a group of independent source queues.
#[derive(Clone, Debug)]
pub struct StreamingInputSender {
    endpoints: Arc<Vec<SourceEndpoint>>,
    cancellation: CancellationToken,
    progress: Arc<Notify>,
}

impl StreamingInputSender {
    fn endpoint(&self, source_id: u32) -> Option<&SourceEndpoint> {
        self.endpoints.get(source_id as usize)
    }

    /// Push while preserving ownership on every non-success result.
    pub fn try_push_batch_owned(
        &self,
        source_id: u32,
        input_id: InputId,
        batch: MicroPartitionRef,
    ) -> Result<(), StreamingPushError> {
        let retry = batch.clone();
        match self.try_push_batch_with(source_id, input_id, || {
            Ok::<_, std::convert::Infallible>(batch)
        }) {
            Ok(()) => Ok(()),
            Err(StreamingBuildPushError::Again) => Err(StreamingPushError::Again(retry)),
            Err(StreamingBuildPushError::End) => Err(StreamingPushError::End(retry)),
            Err(StreamingBuildPushError::InvalidSource) => {
                Err(StreamingPushError::InvalidSource(retry))
            }
            Err(StreamingBuildPushError::Build(never)) => match never {},
        }
    }

    /// Reserve capacity, then construct and atomically enqueue one batch.
    ///
    /// This API prevents an Arrow importer from taking foreign ownership and
    /// subsequently discovering that the queue is full.
    pub fn try_push_batch_with<E, F>(
        &self,
        source_id: u32,
        input_id: InputId,
        build: F,
    ) -> Result<(), StreamingBuildPushError<E>>
    where
        F: FnOnce() -> Result<MicroPartitionRef, E>,
    {
        if self.cancellation.is_cancelled() {
            return Err(StreamingBuildPushError::End);
        }
        let Some(endpoint) = self.endpoint(source_id) else {
            return Err(StreamingBuildPushError::InvalidSource);
        };
        let finished = endpoint.finished.lock().unwrap();
        if *finished || self.cancellation.is_cancelled() {
            return Err(StreamingBuildPushError::End);
        }
        let permit = match endpoint.sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(StreamingBuildPushError::Again);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(StreamingBuildPushError::End);
            }
        };
        let batch = build().map_err(StreamingBuildPushError::Build)?;
        permit.send(StreamingMessage::Batch { input_id, batch });
        Ok(())
    }

    /// Compatibility helper used by low-level queue tests.
    pub fn try_push_batch(&self, source_id: u32, batch: MicroPartitionRef) -> StreamingPushResult {
        self.try_push_batch_owned(source_id, source_id, batch)
            .map(|()| StreamingPushResult::Ok)
            .unwrap_or_else(|error| error.status())
    }

    /// Queue exactly one Finish for a source and input invocation.
    pub fn try_finish_for_input(&self, source_id: u32, input_id: InputId) -> StreamingPushResult {
        if self.cancellation.is_cancelled() {
            return StreamingPushResult::End;
        }
        let Some(endpoint) = self.endpoint(source_id) else {
            return StreamingPushResult::Error;
        };
        let mut finished = endpoint.finished.lock().unwrap();
        if *finished || self.cancellation.is_cancelled() {
            return StreamingPushResult::End;
        }
        match endpoint
            .sender
            .try_send(StreamingMessage::Finish { input_id })
        {
            Ok(()) => {
                *finished = true;
                StreamingPushResult::Ok
            }
            Err(mpsc::error::TrySendError::Full(_)) => StreamingPushResult::Again,
            Err(mpsc::error::TrySendError::Closed(_)) => StreamingPushResult::End,
        }
    }

    pub fn try_finish_source(&self, source_id: u32) -> StreamingPushResult {
        self.try_finish_for_input(source_id, source_id)
    }

    /// Wait until a source consumes an item, cancellation occurs, or timeout.
    pub async fn wait_for_progress(&self, timeout: Duration) -> bool {
        if self.cancellation.is_cancelled() {
            return true;
        }
        tokio::select! {
            _ = self.progress.notified() => true,
            _ = self.cancellation.cancelled() => true,
            _ = tokio::time::sleep(timeout) => false,
        }
    }

    /// Idempotently stop all queues and wake capacity waiters/consumers.
    pub fn cancel(&self) {
        self.cancellation.cancel();
        self.progress.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// Pipeline source backed by bounded external input queues.
pub struct StreamingSource {
    schema: SchemaRef,
    receivers: Vec<mpsc::Receiver<StreamingMessage>>,
    queue_depth: usize,
    cancellation: CancellationToken,
    progress: Arc<Notify>,
}

impl StreamingSource {
    pub fn new(
        schema: SchemaRef,
        num_sources: usize,
        queue_depth: usize,
    ) -> (Self, StreamingInputSender) {
        assert!(
            num_sources > 0,
            "StreamingSource requires at least one source"
        );
        assert!(
            queue_depth > 0,
            "StreamingSource queue depth must be positive"
        );
        let cancellation = CancellationToken::new();
        let progress = Arc::new(Notify::new());
        let mut endpoints = Vec::with_capacity(num_sources);
        let mut receivers = Vec::with_capacity(num_sources);
        for _ in 0..num_sources {
            let (sender, receiver) = mpsc::channel(queue_depth);
            endpoints.push(SourceEndpoint {
                sender,
                finished: Mutex::new(false),
            });
            receivers.push(receiver);
        }
        let sender = StreamingInputSender {
            endpoints: Arc::new(endpoints),
            cancellation: cancellation.clone(),
            progress: progress.clone(),
        };
        (
            Self {
                schema,
                receivers,
                queue_depth,
                cancellation,
                progress,
            },
            sender,
        )
    }

    /// Convert the bounded receivers into the pipeline message stream.
    ///
    /// A `Finish` message is emitted as one `Flush` and then terminates only
    /// that source's stream.  The merged stream terminates after every source
    /// has finished, even while producer handles are still alive.
    pub fn into_stream(self) -> SourceStream<'static> {
        let cancellation = self.cancellation;
        let progress = self.progress;
        let streams = self.receivers.into_iter().map(|receiver| {
            let progress = progress.clone();
            stream::unfold(Some(receiver), move |receiver| {
                let progress = progress.clone();
                async move {
                    let mut receiver = receiver?;
                    let message = receiver.recv().await?;
                    progress.notify_waiters();
                    let (message, next_receiver) = match message {
                        StreamingMessage::Batch { input_id, batch } => (
                            PipelineMessage::Morsel {
                                input_id,
                                partition: Arc::try_unwrap(batch)
                                    .unwrap_or_else(|batch| (*batch).clone()),
                            },
                            Some(receiver),
                        ),
                        StreamingMessage::Finish { input_id } => {
                            (PipelineMessage::Flush(input_id), None)
                        }
                    };
                    Some((Ok(message), next_receiver))
                }
            })
            .boxed()
        });
        stream::select_all(streams)
            .take_until(cancellation.cancelled_owned())
            .boxed()
    }
}

#[async_trait]
impl Source for StreamingSource {
    fn get_data(
        self: Box<Self>,
        _maintain_order: bool,
        _stats_provider: StatsProvider,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        Ok((*self).into_stream())
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn name(&self) -> NodeName {
        "Streaming Source".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::InMemoryScan
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "Streaming Source:".to_string(),
            format!("Sources = {}", self.receivers.len()),
            format!("Queue depth = {}", self.queue_depth),
        ]
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::*;

    use super::*;

    #[tokio::test]
    async fn invalid_source_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32)]));
        let (_source, sender) = StreamingSource::new(schema.clone(), 1, 1);
        let batch = Arc::new(daft_micropartition::MicroPartition::empty(Some(schema)));
        assert_eq!(sender.try_push_batch(9, batch), StreamingPushResult::Error);
    }
}
