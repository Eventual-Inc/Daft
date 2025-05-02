pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;

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
