pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;
pub type ReceiverStream<T> = tokio_stream::wrappers::ReceiverStream<T>;

pub fn create_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
    (sender, receiver)
}

pub type OneshotSender<T> = tokio::sync::oneshot::Sender<T>;
pub type OneshotReceiver<T> = tokio::sync::oneshot::Receiver<T>;

pub fn create_oneshot_channel<T>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    (sender, receiver)
}

pub type UnboundedSender<T> = tokio::sync::mpsc::UnboundedSender<T>;
pub type UnboundedReceiver<T> = tokio::sync::mpsc::UnboundedReceiver<T>;

pub fn create_unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    (sender, receiver)
}
