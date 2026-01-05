use futures::Stream;

pub(crate) struct Sender<T>(tokio::sync::mpsc::Sender<T>);
impl<T> Sender<T> {
    pub(crate) async fn send(&self, val: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.0.send(val).await
    }

    pub(crate) fn try_send(&self, val: T) -> Result<(), tokio::sync::mpsc::error::TrySendError<T>> {
        self.0.try_send(val)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender(self.0.clone())
    }
}

pub(crate) struct Receiver<T>(tokio::sync::mpsc::Receiver<T>);
impl<T> Receiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        self.0.recv().await
    }

    pub(crate) fn into_stream(self) -> impl Stream<Item = T> {
        futures::stream::unfold(self, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
    }
}

pub(crate) fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(buffer_size);
    (Sender(tx), Receiver(rx))
}
