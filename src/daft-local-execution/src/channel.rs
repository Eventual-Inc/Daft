use loole::SendError;

pub(crate) type Sender<T> = loole::Sender<T>;
pub(crate) type Receiver<T> = loole::Receiver<T>;

pub(crate) fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    loole::bounded(buffer_size)
}

pub(crate) fn create_ordering_aware_receiver_channel<T>(
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

pub(crate) fn create_ordering_aware_sender_channel<T>(
    ordered: bool,
    buffer_size: usize,
) -> (OrderingAwareSender<T>, Vec<Receiver<T>>) {
    match ordered {
        true => {
            let (sender, receivers) = (0..buffer_size).map(|_| create_channel::<T>(1)).unzip();
            (
                OrderingAwareSender::InOrder(RoundRobinSender::new(sender)),
                receivers,
            )
        }
        false => {
            let (sender, receiver) = create_channel::<T>(buffer_size);
            (
                OrderingAwareSender::OutOfOrder(sender),
                (0..buffer_size).map(|_| receiver.clone()).collect(),
            )
        }
    }
}

pub(crate) enum OrderingAwareSender<T> {
    InOrder(RoundRobinSender<T>),
    OutOfOrder(Sender<T>),
}

impl<T> OrderingAwareSender<T> {
    pub(crate) async fn send(&mut self, val: T) -> Result<(), SendError<T>> {
        match self {
            Self::InOrder(rr) => rr.send(val).await,
            Self::OutOfOrder(s) => s.send_async(val).await,
        }
    }
}

pub(crate) struct RoundRobinSender<T> {
    senders: Vec<Sender<T>>,
    next_sender_idx: usize,
}

impl<T> RoundRobinSender<T> {
    fn new(senders: Vec<Sender<T>>) -> Self {
        Self {
            senders,
            next_sender_idx: 0,
        }
    }

    async fn send(&mut self, val: T) -> Result<(), SendError<T>> {
        let next_sender_idx = self.next_sender_idx;
        self.next_sender_idx = (next_sender_idx + 1) % self.senders.len();
        self.senders[next_sender_idx].send_async(val).await
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
            Self::OutOfOrder(r) => r.recv_async().await.ok(),
        }
    }
}

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
            if let Ok(val) = self.receivers[next_idx].recv_async().await {
                self.curr_receiver_idx = (next_idx + 1) % self.receivers.len();
                return Some(val);
            }
        }
        self.is_done = true;
        None
    }
}
