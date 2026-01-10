use futures::Stream;

#[derive(Clone)]
pub(crate) struct Sender<T>(kanal::AsyncSender<T>);
impl<T> Sender<T> {
    pub(crate) async fn send(&self, val: T) -> Result<(), kanal::SendError> {
        self.0.send(val).await
    }
}

#[derive(Clone)]
pub(crate) struct Receiver<T>(kanal::AsyncReceiver<T>);
impl<T> Receiver<T> {
    pub(crate) async fn recv(&self) -> Option<T> {
        self.0.recv().await.ok()
    }

    pub(crate) fn into_stream(self) -> impl Stream<Item = T> {
        futures::stream::unfold(
            self,
            |rx| async move { rx.recv().await.map(|item| (item, rx)) },
        )
    }
}

pub(crate) fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = kanal::bounded_async::<T>(buffer_size);
    (Sender(tx), Receiver(rx))
}
