pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;

pub fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    tokio::sync::mpsc::channel(buffer_size)
}

pub(crate) fn create_ordering_aware_receiver_channel<T: Clone>(
    ordered: bool,
    buffer_size: usize,
) -> (Vec<Sender<T>>, OrderingAwareReceiver<T>) {
    match ordered {
        true => {
            let (senders, receiver) = (0..buffer_size).map(|_| create_channel::<T>(1)).unzip();
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

pub(crate) enum OrderingAwareReceiver<T: Clone> {
    InOrder(RoundRobinReceiver<T>),
    OutOfOrder(Receiver<T>),
}

impl<T: Clone> OrderingAwareReceiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        match self {
            Self::InOrder(rr) => rr.recv().await,
            Self::OutOfOrder(r) => r.recv().await,
        }
    }
}

pub(crate) struct RoundRobinReceiver<T: Clone> {
    receivers: Vec<Receiver<T>>,
    curr_receiver_idx: usize,
    is_done: bool,
}

impl<T: Clone> RoundRobinReceiver<T> {
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
