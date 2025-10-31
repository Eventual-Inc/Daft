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

/// A multi-producer, single-consumer channel that is aware of the ordering of the senders.
/// If `ordered` is true, the receiver will try to receive from each sender in a round-robin fashion.
/// This is useful when collecting results from multiple workers in a specific order.
pub(crate) fn create_ordering_aware_receiver_channel<T: Clone>(
    ordered: bool,
    buffer_size: usize,
) -> (Vec<Sender<T>>, OrderingAwareReceiver<T>) {
    match ordered {
        true => {
            let (senders, receiver) = (0..buffer_size).map(|_| create_channel::<T>(0)).unzip();
            (
                senders,
                OrderingAwareReceiver::InOrder(RoundRobinReceiver::new(receiver)),
            )
        }
        false => {
            let (sender, receiver) = create_channel::<T>(buffer_size);
            (
                (0..buffer_size).map(|_| sender.clone()).collect(),
                OrderingAwareReceiver::OutOfOrder(receiver),
            )
        }
    }
}

pub(crate) enum OrderingAwareReceiver<T> {
    InOrder(RoundRobinReceiver<T>),
    OutOfOrder(Receiver<T>),
}

impl<T> OrderingAwareReceiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        match self {
            Self::InOrder(rr) => rr.recv().await,
            Self::OutOfOrder(r) => r.recv().await,
        }
    }
}

/// A round-robin receiver that tries to receive from each receiver in a round-robin fashion.
pub(crate) struct RoundRobinReceiver<T> {
    receivers: Vec<Receiver<T>>,
    curr_receiver_idx: usize,
    is_done: bool,
}

impl<T> RoundRobinReceiver<T> {
    fn new(receivers: Vec<Receiver<T>>) -> Self {
        Self {
            receivers,
            curr_receiver_idx: 0,
            is_done: false,
        }
    }

    async fn recv(&mut self) -> Option<T> {
        if self.is_done {
            return None;
        }
        for i in 0..self.receivers.len() {
            let next_idx = (i + self.curr_receiver_idx) % self.receivers.len();
            if let Some(val) = self.receivers[next_idx].recv().await {
                self.curr_receiver_idx = (next_idx + 1) % self.receivers.len();
                return Some(val);
            }
        }
        self.is_done = true;
        None
    }
}
