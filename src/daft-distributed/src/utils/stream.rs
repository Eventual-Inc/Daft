use std::{
    pin::Pin,
    task::{Context, Poll},
};

use common_error::DaftResult;
use common_runtime::JoinSet;
use futures::{Stream, StreamExt};

pub(crate) struct JoinableForwardingStream<S: Stream + Send + Unpin + 'static> {
    input_stream: Option<S>,
    joinset: Option<JoinSet<DaftResult<()>>>,
}

impl<S> JoinableForwardingStream<S>
where
    S: Stream + Send + Unpin + 'static,
{
    pub fn new(input_stream: S, joinset: JoinSet<DaftResult<()>>) -> Self {
        Self {
            input_stream: Some(input_stream),
            joinset: Some(joinset),
        }
    }
}

impl<S> Stream for JoinableForwardingStream<S>
where
    S: Stream + Send + Unpin + 'static,
{
    type Item = DaftResult<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if let Some(input_stream) = this.input_stream.as_mut() {
            match input_stream.poll_next_unpin(cx) {
                // Preserve the existing input priority when data is ready.
                Poll::Ready(Some(result)) => return Poll::Ready(Some(Ok(result))),
                Poll::Ready(None) => this.input_stream = None,
                // Register the input waker before checking background tasks.
                Poll::Pending => {}
            }
        }

        // Keep draining so every remaining task registers this stream's waker.
        while let Some(joinset) = this.joinset.as_mut() {
            match joinset.poll_join_next(cx) {
                Poll::Ready(Some(Ok(Ok(())))) => {}
                Poll::Ready(Some(Ok(Err(e)) | Err(e))) => {
                    // Stop forwarding input, but retain the remaining tasks so they
                    // can observe the closed input channel and complete cleanup.
                    this.input_stream = None;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => this.joinset = None,
                Poll::Pending => return Poll::Pending,
            }
        }

        if this.input_stream.is_some() {
            Poll::Pending
        } else {
            Poll::Ready(None)
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
    async fn test_joinable_forwarding_stream_error_allows_remaining_tasks_to_cleanup() {
        let (release_cleanup_tx, release_cleanup_rx) = tokio::sync::oneshot::channel();
        let (cleanup_done_tx, cleanup_done_rx) = tokio::sync::oneshot::channel();

        let mut joinset = JoinSet::new();
        joinset.spawn(async move { Err(DaftError::InternalError("test error".to_string())) });
        joinset.spawn(async move {
            release_cleanup_rx.await.unwrap();
            cleanup_done_tx.send(()).unwrap();
            Ok(())
        });

        // Ensure the error is ready while the cleanup task remains blocked.
        tokio::task::yield_now().await;

        let mut stream = JoinableForwardingStream::new(futures::stream::empty::<usize>(), joinset);
        let result = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("background error should be surfaced")
            .expect("stream should emit an error item");
        assert!(result.unwrap_err().to_string().contains("test error"));

        release_cleanup_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), cleanup_done_rx)
            .await
            .expect("remaining background tasks should not be aborted")
            .expect("cleanup task should finish normally");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_joinable_forwarding_stream_active_error_stops_input_and_keeps_cleanup_running() {
        let (input_tx, input_rx) = create_channel(1);
        let (release_cleanup_tx, release_cleanup_rx) = tokio::sync::oneshot::channel();
        let (cleanup_done_tx, cleanup_done_rx) = tokio::sync::oneshot::channel();

        let mut joinset = JoinSet::new();
        joinset.spawn(async move { Err(DaftError::InternalError("test error".to_string())) });
        joinset.spawn(async move {
            release_cleanup_rx.await.unwrap();
            cleanup_done_tx.send(()).unwrap();
            Ok(())
        });

        tokio::task::yield_now().await;

        let mut stream = JoinableForwardingStream::new(
            tokio_stream::wrappers::ReceiverStream::new(input_rx),
            joinset,
        );
        let result = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("background error should be surfaced while input is pending")
            .expect("stream should emit an error item");
        assert!(result.unwrap_err().to_string().contains("test error"));

        // The input receiver is dropped as part of fail-fast, while cleanup tasks keep
        // running independently even if the consumer does not poll the stream again.
        assert!(input_tx.send(1).await.is_err());
        release_cleanup_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), cleanup_done_rx)
            .await
            .expect("remaining background tasks should continue without another stream poll")
            .expect("cleanup task should finish normally");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_joinable_forwarding_stream_preserves_ready_input_before_background_error() {
        let mut joinset = JoinSet::new();
        joinset.spawn(async move { Err(DaftError::InternalError("test error".to_string())) });
        tokio::task::yield_now().await;

        let mut stream = JoinableForwardingStream::new(futures::stream::iter([42usize]), joinset);
        assert_eq!(stream.next().await.unwrap().unwrap(), 42);
        assert!(
            stream
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("test error")
        );
        assert!(stream.next().await.is_none());
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
                } else if tx.send(1).await.is_err() {
                    return Ok(());
                }
                Ok(())
            });
        }
        drop(tx);

        let mut stream =
            JoinableForwardingStream::new(tokio_stream::wrappers::ReceiverStream::new(rx), joinset);

        let mut count = 0;
        let mut saw_error = false;
        while let Some(result) = stream.next().await {
            if let Err(e) = result {
                assert!(matches!(e, DaftError::InternalError(_)));
                assert!(e.to_string().contains("test error"));
                saw_error = true;
            } else {
                assert_eq!(result.unwrap(), 1);
                count += 1;
            }
        }
        assert!(stream.next().await.is_none());
        assert!(saw_error);
        assert!(count <= 9);
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
                } else if tx.send(1).await.is_err() {
                    return Ok(());
                }
                Ok(())
            });
        }
        drop(tx);

        let mut stream =
            JoinableForwardingStream::new(tokio_stream::wrappers::ReceiverStream::new(rx), joinset);

        let mut count = 0;
        let mut saw_panic = false;
        while let Some(result) = stream.next().await {
            if let Err(e) = result {
                assert!(matches!(e, DaftError::JoinError(_)));
                assert!(e.to_string().contains("test panic"));
                saw_panic = true;
            } else {
                assert_eq!(result.unwrap(), 1);
                count += 1;
            }
        }
        assert!(stream.next().await.is_none());
        assert!(saw_panic);
        assert!(count <= 9);
    }
}
