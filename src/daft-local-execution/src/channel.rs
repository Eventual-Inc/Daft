use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

pub type SingleSender = tokio::sync::mpsc::Sender<DaftResult<Arc<MicroPartition>>>;
pub type SingleReceiver = tokio::sync::mpsc::Receiver<DaftResult<Arc<MicroPartition>>>;

pub fn spawn_compute_task<F>(future: F)
where
    F: std::future::Future<Output = DaftResult<()>> + Send + 'static,
{
    tokio::spawn(async move {
        let _ = future.await;
    });
}

pub fn create_single_channel(buffer_size: usize) -> (SingleSender, SingleReceiver) {
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
    InOrder(InOrderSender),
    OutOfOrder(OutOfOrderSender),
}

impl MultiSender {
    pub fn get_next_sender(&mut self) -> SingleSender {
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
pub struct InOrderSender {
    senders: Vec<SingleSender>,
    curr_sender_idx: usize,
}

impl InOrderSender {
    pub fn new(senders: Vec<SingleSender>) -> Self {
        Self {
            senders,
            curr_sender_idx: 0,
        }
    }

    pub fn get_next_sender(&mut self) -> SingleSender {
        let next_idx = self.curr_sender_idx;
        self.curr_sender_idx = (next_idx + 1) % self.senders.len();
        self.senders[next_idx].clone()
    }
}

pub struct OutOfOrderSender {
    sender: SingleSender,
}

impl OutOfOrderSender {
    pub fn new(sender: SingleSender) -> Self {
        Self { sender }
    }

    pub fn get_sender(&self) -> SingleSender {
        self.sender.clone()
    }
}

pub enum MultiReceiver {
    InOrder(InOrderReceiver),
    OutOfOrder(OutOfOrderReceiver),
}

impl MultiReceiver {
    pub async fn recv(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        match self {
            Self::InOrder(receiver) => receiver.recv().await,
            Self::OutOfOrder(receiver) => receiver.recv().await,
        }
    }
}

pub struct InOrderReceiver {
    receivers: Vec<SingleReceiver>,
    curr_receiver_idx: usize,
    is_done: bool,
}

impl InOrderReceiver {
    pub fn new(receivers: Vec<SingleReceiver>) -> Self {
        Self {
            receivers,
            curr_receiver_idx: 0,
            is_done: false,
        }
    }

    pub async fn recv(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
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

pub struct OutOfOrderReceiver {
    receiver: SingleReceiver,
}

impl OutOfOrderReceiver {
    pub fn new(receiver: SingleReceiver) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if let Some(val) = self.receiver.recv().await {
            return Some(val);
        }
        None
    }
}
