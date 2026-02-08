use futures::Stream;

pub(crate) struct Sender<T>(tokio::sync::mpsc::Sender<T>);

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> Sender<T> {
    pub(crate) async fn send(&self, val: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.0.send(val).await
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

pub(crate) struct Receiver<T>(tokio::sync::mpsc::Receiver<T>);
impl<T> Receiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        self.0.recv().await
    }

    pub(crate) fn into_stream(self) -> impl Stream<Item = T> {
        tokio_stream::wrappers::ReceiverStream::new(self.0)
    }
}

pub(crate) fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(buffer_size);
    (Sender(tx), Receiver(rx))
}
