use std::{
    pin::Pin,
    task::{Context, Poll},
};

use common_error::DaftResult;
use futures::{Stream, StreamExt};

use crate::utils::joinset::JoinSet;

#[derive(Debug)]
enum ForwardingStreamState<S: Stream + Send + Unpin + 'static> {
    // Active: Forwarding results from input stream and tracking background tasks
    Active {
        input_stream: S,
        joinset: Option<JoinSet<DaftResult<()>>>,
    },
    // AwaitingTasks: Input stream is done, awaiting background tasks to complete
    AwaitingTasks(JoinSet<DaftResult<()>>),
    // Complete: Both stream and background tasks are finished
    Complete,
}

pub(crate) struct JoinableForwardingStream<S: Stream + Send + Unpin + 'static> {
    state: ForwardingStreamState<S>,
}

impl<S> JoinableForwardingStream<S>
where
    S: Stream + Send + Unpin + 'static,
{
    pub fn new(input_stream: S, joinset: JoinSet<DaftResult<()>>) -> Self {
        Self {
            state: ForwardingStreamState::Active {
                input_stream,
                joinset: Some(joinset),
            },
        }
    }
}

impl<S> Stream for JoinableForwardingStream<S>
where
    S: Stream + Send + Unpin + 'static,
{
    type Item = DaftResult<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        fn poll_inner<S>(
            state: &mut ForwardingStreamState<S>,
            cx: &mut Context<'_>,
        ) -> Option<Poll<Option<DaftResult<S::Item>>>>
        where
            S: Stream + Send + Unpin + 'static,
        {
            match state {
                // Active: Forwarding results from input stream and tracking background tasks
                ForwardingStreamState::Active {
                    input_stream,
                    joinset,
                } => {
                    match input_stream.poll_next_unpin(cx) {
                        // Received a result from the stream, forward it
                        Poll::Ready(Some(result)) => Some(Poll::Ready(Some(Ok(result)))),
                        // Input stream is done, transition to awaiting tasks
                        Poll::Ready(None) => {
                            let joinset = joinset.take().expect("JoinSet should exist");
                            *state = ForwardingStreamState::AwaitingTasks(joinset);
                            None
                        }
                        // Still waiting for more results from the stream
                        Poll::Pending => Some(Poll::Pending),
                    }
                }
                // AwaitingTasks: Input stream is done, awaiting background tasks to complete
                ForwardingStreamState::AwaitingTasks(joinset) => match joinset.poll_join_next(cx) {
                    // Received a result from a background task
                    Poll::Ready(Some(result)) => match result {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => Some(Poll::Ready(Some(Err(e)))),
                        Err(e) => Some(Poll::Ready(Some(Err(e)))),
                    },
                    // All background tasks are complete
                    Poll::Ready(None) => {
                        *state = ForwardingStreamState::Complete;
                        None
                    }
                    // Still waiting for background tasks to complete
                    Poll::Pending => Some(Poll::Pending),
                },
                // Complete: Both stream and background tasks are finished
                ForwardingStreamState::Complete => Some(Poll::Ready(None)),
            }
        }

        loop {
            if let Some(poll) = poll_inner(&mut self.state, cx) {
                return poll;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftError;

    use super::*;
    use crate::utils::channel::create_channel;

    #[tokio::test]
    async fn test_joinable_forwarding_stream_basic() {
        let (tx, rx) = create_channel(1);

        let mut joinset = JoinSet::new();
        for i in 0..10 {
            let tx = tx.clone();
            joinset.spawn(async move {
                tx.send(i).await.unwrap();
                Ok(())
            });
        }
        drop(tx);

        let mut stream =
            JoinableForwardingStream::new(tokio_stream::wrappers::ReceiverStream::new(rx), joinset);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            assert_eq!(result.unwrap(), count);
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_joinable_forwarding_stream_basic_error() {
        let (tx, rx) = create_channel(1);

        let mut joinset = JoinSet::new();
        for i in 0..10 {
            let tx = tx.clone();
            joinset.spawn(async move {
                if i == 5 {
                    return Err(DaftError::InternalError("test error".to_string()));
                } else {
                    tx.send(1).await.unwrap();
                }
                Ok(())
            });
        }
        drop(tx);

        let mut stream =
            JoinableForwardingStream::new(tokio_stream::wrappers::ReceiverStream::new(rx), joinset);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            if let Err(e) = result {
                assert!(matches!(e, DaftError::InternalError(_)));
                assert!(e.to_string().contains("test error"));
            } else {
                assert_eq!(result.unwrap(), 1);
                count += 1;
            }
        }
        assert!(stream.next().await.is_none());
        // 9 results because we consume the stream before joining the tasks
        assert_eq!(count, 9);
    }

    #[tokio::test]
    async fn test_joinable_forwarding_stream_basic_panic() {
        let (tx, rx) = create_channel(1);

        let mut joinset = JoinSet::new();
        for i in 0..10 {
            let tx = tx.clone();
            joinset.spawn(async move {
                if i == 5 {
                    panic!("test panic");
                } else {
                    tx.send(1).await.unwrap();
                }
                Ok(())
            });
        }
        drop(tx);

        let mut stream =
            JoinableForwardingStream::new(tokio_stream::wrappers::ReceiverStream::new(rx), joinset);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            if let Err(e) = result {
                assert!(matches!(e, DaftError::External(_)));
                assert!(e.to_string().contains("test panic"));
            } else {
                assert_eq!(result.unwrap(), 1);
                count += 1;
            }
        }
        assert!(stream.next().await.is_none());
        // 9 results because we consume the stream before joining the tasks
        assert_eq!(count, 9);
    }
}
