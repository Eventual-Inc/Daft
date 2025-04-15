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

    pub(crate) fn into_inner(self) -> kanal::AsyncReceiver<T> {
        self.0
    }
}

pub(crate) fn create_channel<T: Clone>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = kanal::bounded_async::<T>(buffer_size);
    (Sender(tx), Receiver(rx))
}
