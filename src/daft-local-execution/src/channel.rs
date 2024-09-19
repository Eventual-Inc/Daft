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
    sender: Sender<PipelineResultType>,
    receiver: Receiver<PipelineResultType>,
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

    pub(crate) fn get_receiver_with_stats(
        self,
        stats: &Arc<RuntimeStatsContext>,
    ) -> CountingReceiver {
        CountingReceiver::new(self.receiver, stats.clone())
    }

    pub(crate) fn get_receiver(self) -> Receiver<PipelineResultType> {
        self.receiver
    }
}

pub(crate) fn make_ordering_aware_channel<T>(
    buffer_size: usize,
    ordered: bool,
) -> (Vec<Sender<T>>, OrderingAwareReceiver<T>) {
    match ordered {
        true => {
            let (senders, receivers) = (0..buffer_size).map(|_| create_channel(1)).unzip();
            (
                senders,
                OrderingAwareReceiver::Ordered(RoundRobinReceiver::new(receivers)),
            )
        }
        false => {
            let (sender, receiver) = create_channel(buffer_size);
            (
                (0..buffer_size).map(|_| sender.clone()).collect(),
                OrderingAwareReceiver::Unordered(receiver),
            )
        }
    }
}

pub enum OrderingAwareReceiver<T> {
    Ordered(RoundRobinReceiver<T>),
    Unordered(Receiver<T>),
}

impl<T> OrderingAwareReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
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
