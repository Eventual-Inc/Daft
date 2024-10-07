use std::sync::Arc;

use crate::{
    pipeline::PipelineResultType,
    runtime_stats::{CountingReceiver, CountingSender, RuntimeStatsContext},
};

pub type Sender<T> = tokio::sync::mpsc::Sender<T>;
pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;

pub fn create_channel<T>(buffer_size: usize) -> (Sender<T>, Receiver<T>) {
    tokio::sync::mpsc::channel(buffer_size)
}

pub struct PipelineChannel {
    sender: PipelineSender,
    receiver: PipelineReceiver,
}

impl PipelineChannel {
    pub fn new(buffer_size: usize, in_order: bool) -> Self {
        if in_order {
            let (senders, receivers) = (0..buffer_size).map(|_| create_channel(1)).unzip();
            let sender = PipelineSender::InOrder(RoundRobinSender::new(senders));
            let receiver = PipelineReceiver::InOrder(RoundRobinReceiver::new(receivers));
            Self { sender, receiver }
        } else {
            let (sender, receiver) = create_channel(buffer_size);
            let sender = PipelineSender::OutOfOrder(sender);
            let receiver = PipelineReceiver::OutOfOrder(receiver);
            Self { sender, receiver }
        }
    }

    fn get_next_sender(&mut self) -> Sender<PipelineResultType> {
        match &mut self.sender {
            PipelineSender::InOrder(rr) => rr.get_next_sender(),
            PipelineSender::OutOfOrder(sender) => sender.clone(),
        }
    }

    pub(crate) fn get_next_sender_with_stats(
        &mut self,
        rt: &Arc<RuntimeStatsContext>,
    ) -> CountingSender {
        CountingSender::new(self.get_next_sender(), rt.clone())
    }

    pub fn get_receiver(self) -> PipelineReceiver {
        self.receiver
    }

    pub(crate) fn get_receiver_with_stats(self, rt: &Arc<RuntimeStatsContext>) -> CountingReceiver {
        CountingReceiver::new(self.get_receiver(), rt.clone())
    }
}

pub enum PipelineSender {
    InOrder(RoundRobinSender<PipelineResultType>),
    OutOfOrder(Sender<PipelineResultType>),
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

pub enum PipelineReceiver {
    InOrder(RoundRobinReceiver<PipelineResultType>),
    OutOfOrder(Receiver<PipelineResultType>),
}

impl PipelineReceiver {
    pub async fn recv(&mut self) -> Option<PipelineResultType> {
        match self {
            Self::InOrder(rr) => rr.recv().await,
            Self::OutOfOrder(r) => r.recv().await,
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
