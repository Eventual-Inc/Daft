use std::future::Future;

use futures::{Stream, StreamExt};

// Helper function to combine a stream with a future that returns a result
pub(crate) fn combine_stream<T, E>(
    stream: impl Stream<Item = Result<T, E>> + Unpin,
    future: impl Future<Output = Result<Result<(), E>, E>>,
) -> impl Stream<Item = Result<T, E>> {
    use futures::stream::unfold;

    let initial_state = (Some(future), stream);

    unfold(initial_state, |(future, mut stream)| async move {
        future.as_ref()?;

        match stream.next().await {
            Some(item) => Some((item, (future, stream))),
            None => match future.unwrap().await {
                Err(error) => Some((Err(error), (None, stream))),
                Ok(Err(error)) => Some((Err(error), (None, stream))),
                Ok(Ok(())) => None,
            },
        }
    })
}
