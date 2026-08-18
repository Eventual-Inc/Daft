use std::{
    pin::Pin,
    task::{Context, Poll},
};

use common_error::DaftResult;
use common_runtime::JoinSet;
use futures::{Stream, StreamExt};

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
                    let mut joinset_complete = false;
                    if let Some(active_joinset) = joinset.as_mut() {
                        loop {
                            match active_joinset.poll_join_next(cx) {
                                // Keep polling after successful completions so the remaining
                                // tasks register this stream's waker before we return Pending.
                                Poll::Ready(Some(Ok(Ok(())))) => {}
                                Poll::Ready(Some(Ok(Err(e)))) => {
                                    active_joinset.abort_all();
                                    *state = ForwardingStreamState::Complete;
                                    return Some(Poll::Ready(Some(Err(e))));
                                }
                                Poll::Ready(Some(Err(e))) => {
                                    active_joinset.abort_all();
                                    *state = ForwardingStreamState::Complete;
                                    return Some(Poll::Ready(Some(Err(e))));
                                }
                                Poll::Ready(None) => {
                                    joinset_complete = true;
                                    break;
                                }
                                Poll::Pending => break,
                            }
                        }
                    }
                    if joinset_complete {
                        *joinset = None;
                    }

                    match input_stream.poll_next_unpin(cx) {
                        // Received a result from the stream, forward it.
                        Poll::Ready(Some(result)) => Some(Poll::Ready(Some(Ok(result)))),
                        // Input stream is done, transition to awaiting tasks.
                        Poll::Ready(None) => {
                            *state = match joinset.take() {
                                Some(joinset) => ForwardingStreamState::AwaitingTasks(joinset),
                                None => ForwardingStreamState::Complete,
                            };
                            None
                        }
                        // Still waiting for more results from the stream.
                        Poll::Pending => Some(Poll::Pending),
                    }
                }
                // AwaitingTasks: Input stream is done, awaiting background tasks to complete
                ForwardingStreamState::AwaitingTasks(joinset) => match joinset.poll_join_next(cx) {
                    // Received a result from a background task
                    Poll::Ready(Some(result)) => match result {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => {
                            joinset.abort_all();
                            *state = ForwardingStreamState::Complete;
                            Some(Poll::Ready(Some(Err(e))))
                        }
                        Err(e) => {
                            joinset.abort_all();
                            *state = ForwardingStreamState::Complete;
                            Some(Poll::Ready(Some(Err(e))))
                        }
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
    use std::time::Duration;

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
    async fn test_joinable_forwarding_stream_surfaces_background_error_while_input_pending() {
        let mut joinset = JoinSet::new();
        joinset.spawn(async move { Err(DaftError::InternalError("test error".to_string())) });

        let input_stream = futures::stream::pending::<usize>();
        let mut stream = JoinableForwardingStream::new(input_stream, joinset);

        let result = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("background error should be surfaced while input is pending")
            .expect("stream should emit an error item");

        assert!(matches!(&result, Err(DaftError::InternalError(_))));
        assert!(result.unwrap_err().to_string().contains("test error"));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_joinable_forwarding_stream_keeps_pending_after_background_success() {
        let mut joinset = JoinSet::new();
        joinset.spawn(async move { Ok(()) });

        let input_stream = futures::stream::pending::<usize>();
        let mut stream = JoinableForwardingStream::new(input_stream, joinset);

        let result = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(
            result.is_err(),
            "background success must not end a pending input stream"
        );
    }

    #[tokio::test]
    async fn test_joinable_forwarding_stream_wakes_for_error_after_background_success() {
        let (error_tx, error_rx) = tokio::sync::oneshot::channel();
        let mut joinset = JoinSet::new();
        joinset.spawn(async move { Ok(()) });
        joinset.spawn(async move {
            error_rx.await.unwrap();
            Err(DaftError::InternalError("delayed test error".to_string()))
        });

        // Let the successful task finish before the stream is first polled.
        tokio::task::yield_now().await;

        let input_stream = futures::stream::pending::<usize>();
        let mut stream = JoinableForwardingStream::new(input_stream, joinset);
        let next = stream.next();
        tokio::pin!(next);

        assert!(futures::poll!(&mut next).is_pending());
        error_tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), next)
            .await
            .expect("delayed background error should wake the stream")
            .expect("stream should emit an error item");

        assert!(matches!(&result, Err(DaftError::InternalError(_))));
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("delayed test error")
        );
    }

    #[tokio::test]
    async fn test_joinable_forwarding_stream_preserves_input_after_background_tasks_complete() {
        let mut joinset = JoinSet::new();
        for _ in 0..3 {
            joinset.spawn(async move { Ok(()) });
        }

        let input_stream = futures::stream::iter([1, 2, 3]);
        let stream = JoinableForwardingStream::new(input_stream, joinset);
        let results = stream.collect::<Vec<_>>().await;

        assert_eq!(
            results.into_iter().collect::<DaftResult<Vec<_>>>().unwrap(),
            vec![1, 2, 3]
        );
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
        // The stream should fail fast once a background task errors instead of draining
        // all remaining input items first.
        assert!(count < 9);
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
                assert!(matches!(e, DaftError::JoinError(_)));
                assert!(e.to_string().contains("test panic"));
            } else {
                assert_eq!(result.unwrap(), 1);
                count += 1;
            }
        }
        assert!(stream.next().await.is_none());
        // The stream should fail fast once a background task panics instead of draining
        // all remaining input items first.
        assert!(count < 9);
    }
}
