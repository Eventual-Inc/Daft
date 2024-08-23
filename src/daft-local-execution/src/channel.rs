pub type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
pub type OneShotReceiver<T> = tokio::sync::oneshot::Receiver<T>;

pub fn create_one_shot_channel<T>() -> (OneShotSender<T>, OneShotReceiver<T>) {
    tokio::sync::oneshot::channel()
}

pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;

pub fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    tokio::sync::mpsc::channel(buffer_size)
}

pub fn create_multi_channel<T>(
    buffer_size: usize,
    in_order: bool,
) -> (MultiSender<T>, MultiReceiver<T>) {
    if in_order {
        let (senders, receivers) = (0..buffer_size).map(|_| create_channel(1)).unzip();
        let sender = MultiSender::InOrder(RoundRobinSender::new(senders));
        let receiver = MultiReceiver::InOrder(RoundRobinReceiver::new(receivers));
        (sender, receiver)
    } else {
        let (sender, receiver) = create_channel(buffer_size);
        let sender = MultiSender::OutOfOrder(sender);
        let receiver = MultiReceiver::OutOfOrder(receiver);
        (sender, receiver)
    }
}

pub enum MultiSender<T> {
    InOrder(RoundRobinSender<T>),
    OutOfOrder(Sender<T>),
}

impl<T> MultiSender<T> {
    pub fn get_next_sender(&mut self) -> Sender<T> {
        match self {
            Self::InOrder(sender) => sender.get_next_sender(),
            Self::OutOfOrder(sender) => sender.clone(),
        }
    }
}
pub struct RoundRobinSender<T> {
    senders: Vec<Sender<T>>,
    curr_sender_idx: usize,
}

impl<T> RoundRobinSender<T> {
    pub fn new(senders: Vec<Sender<T>>) -> Self {
        Self {
            senders,
            curr_sender_idx: 0,
        }
    }

    pub fn get_next_sender(&mut self) -> Sender<T> {
        let next_idx = self.curr_sender_idx;
        self.curr_sender_idx = (next_idx + 1) % self.senders.len();
        self.senders[next_idx].clone()
    }
}

pub enum MultiReceiver<T> {
    InOrder(RoundRobinReceiver<T>),
    OutOfOrder(Receiver<T>),
}

impl<T> MultiReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        match self {
            Self::InOrder(receiver) => receiver.recv().await,
            Self::OutOfOrder(receiver) => receiver.recv().await,
        }
    }
}

pub struct RoundRobinReceiver<T> {
    receivers: Vec<Receiver<T>>,
    curr_receiver_idx: usize,
    is_done: bool,
}

impl<T> RoundRobinReceiver<T> {
    pub fn new(receivers: Vec<Receiver<T>>) -> Self {
        Self {
            receivers,
            curr_receiver_idx: 0,
            is_done: false,
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
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
