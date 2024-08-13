use crate::pipeline::PipelineOutput;

pub type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
pub type OneShotReceiver<T> = tokio::sync::oneshot::Receiver<T>;

pub fn create_one_shot_channel<T>() -> (OneShotSender<T>, OneShotReceiver<T>) {
    tokio::sync::oneshot::channel()
}

pub type SingleSender<T> = tokio::sync::mpsc::Sender<T>;
pub type SingleReceiver<T> = tokio::sync::mpsc::Receiver<T>;

pub fn create_single_channel<T>(buffer_size: usize) -> (SingleSender<T>, SingleReceiver<T>) {
    tokio::sync::mpsc::channel(buffer_size)
}

pub fn create_channel(buffer_size: usize, in_order: bool) -> (MultiSender, MultiReceiver) {
    if in_order {
        let (senders, receivers) = (0..buffer_size).map(|_| create_single_channel(1)).unzip();
        let sender = MultiSender::InOrder(InOrderSender::new(senders));
        let receiver = MultiReceiver::InOrder(InOrderReceiver::new(receivers));
        (sender, receiver)
    } else {
        let (sender, receiver) = create_single_channel(buffer_size);
        let sender = MultiSender::OutOfOrder(OutOfOrderSender::new(sender));
        let receiver = MultiReceiver::OutOfOrder(OutOfOrderReceiver::new(receiver));
        (sender, receiver)
    }
}

pub enum MultiSender {
    InOrder(InOrderSender<PipelineOutput>),
    OutOfOrder(OutOfOrderSender<PipelineOutput>),
}

impl MultiSender {
    pub fn get_next_sender(&mut self) -> SingleSender<PipelineOutput> {
        match self {
            Self::InOrder(sender) => sender.get_next_sender(),
            Self::OutOfOrder(sender) => sender.get_sender(),
        }
    }

    pub fn buffer_size(&self) -> usize {
        match self {
            Self::InOrder(sender) => sender.senders.len(),
            Self::OutOfOrder(sender) => sender.sender.max_capacity(),
        }
    }

    pub fn in_order(&self) -> bool {
        match self {
            Self::InOrder(_) => true,
            Self::OutOfOrder(_) => false,
        }
    }
}
pub struct InOrderSender<T> {
    senders: Vec<SingleSender<T>>,
    curr_sender_idx: usize,
}

impl<T> InOrderSender<T> {
    pub fn new(senders: Vec<SingleSender<T>>) -> Self {
        Self {
            senders,
            curr_sender_idx: 0,
        }
    }

    pub fn get_next_sender(&mut self) -> SingleSender<T> {
        let next_idx = self.curr_sender_idx;
        self.curr_sender_idx = (next_idx + 1) % self.senders.len();
        self.senders[next_idx].clone()
    }
}

pub struct OutOfOrderSender<T> {
    sender: SingleSender<T>,
}

impl<T> OutOfOrderSender<T> {
    pub fn new(sender: SingleSender<T>) -> Self {
        Self { sender }
    }

    pub fn get_sender(&self) -> SingleSender<T> {
        self.sender.clone()
    }
}

pub enum MultiReceiver {
    InOrder(InOrderReceiver<PipelineOutput>),
    OutOfOrder(OutOfOrderReceiver<PipelineOutput>),
}

impl MultiReceiver {
    pub async fn recv(&mut self) -> Option<PipelineOutput> {
        match self {
            Self::InOrder(receiver) => receiver.recv().await,
            Self::OutOfOrder(receiver) => receiver.recv().await,
        }
    }
}

pub struct InOrderReceiver<T> {
    receivers: Vec<SingleReceiver<T>>,
    curr_receiver_idx: usize,
    is_done: bool,
}

impl<T> InOrderReceiver<T> {
    pub fn new(receivers: Vec<SingleReceiver<T>>) -> Self {
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

pub struct OutOfOrderReceiver<T> {
    receiver: SingleReceiver<T>,
}

impl<T> OutOfOrderReceiver<T> {
    pub fn new(receiver: SingleReceiver<T>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<T> {
        if let Some(val) = self.receiver.recv().await {
            return Some(val);
        }
        None
    }
}
