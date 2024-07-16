use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::{
    create_channel, create_single_channel, MultiReceiver, MultiSender, SingleReceiver,
    SingleSender, NUM_CPUS,
};

pub trait IntermediateOperator: dyn_clone::DynClone + Send + Sync {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>>;
    fn name(&self) -> &'static str;
}

dyn_clone::clone_trait_object!(IntermediateOperator);

/// The number of rows that will trigger an intermediate operator to output its data.
pub const OUTPUT_THRESHOLD: usize = 1000;

/// State of an operator task, used to buffer data and output it when a threshold is reached.
pub struct OperatorTaskState {
    pub buffer: Vec<Arc<MicroPartition>>,
    pub curr_len: usize,
    pub threshold: usize,
}

impl OperatorTaskState {
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            curr_len: 0,
            threshold: OUTPUT_THRESHOLD,
        }
    }

    pub fn add(&mut self, part: Arc<MicroPartition>) {
        self.buffer.push(part);
        self.curr_len += 1;
    }

    pub fn try_clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.curr_len >= self.threshold {
            self.clear()
        } else {
            None
        }
    }

    pub fn clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            return None;
        }
        let concated =
            MicroPartition::concat(&self.buffer.iter().map(|x| x.as_ref()).collect::<Vec<_>>())
                .map(Arc::new);
        self.buffer.clear();
        self.curr_len = 0;
        Some(concated)
    }
}

pub struct IntermediateOpActor {
    sender: MultiSender,
    receiver: MultiReceiver,
    op: Box<dyn IntermediateOperator>,
}

impl IntermediateOpActor {
    pub fn new(
        op: Box<dyn IntermediateOperator>,
        receiver: MultiReceiver,
        sender: MultiSender,
    ) -> Self {
        Self {
            op,
            receiver,
            sender,
        }
    }

    async fn single_operator_task(
        mut receiver: SingleReceiver,
        sender: SingleSender,
        op: Box<dyn IntermediateOperator>,
    ) -> DaftResult<()> {
        let mut state = OperatorTaskState::new();
        while let Some(morsel) = receiver.recv().await {
            let result = op.execute(&morsel?)?;
            state.add(result);
            if let Some(part) = state.try_clear() {
                let _ = sender.send(part).await;
            }
        }
        if let Some(part) = state.clear() {
            let _ = sender.send(part).await;
        }
        Ok(())
    }

    pub async fn run(&mut self) -> DaftResult<()> {
        let mut inner_task_senders: Vec<SingleSender> =
            Vec::with_capacity(self.sender.buffer_size());
        let mut curr_sender_idx = 0;

        while let Some(morsel) = self.receiver.recv().await {
            if let Some(s) = inner_task_senders.get(curr_sender_idx) {
                let _ = s.send(morsel).await;
            } else {
                let next_sender = self.sender.get_next_sender();
                let (single_sender, single_receiver) = create_single_channel(1);

                let op = self.op.clone();
                tokio::spawn(async move {
                    let _ = Self::single_operator_task(single_receiver, next_sender, op).await;
                });
                let _ = single_sender.send(morsel).await;

                inner_task_senders.push(single_sender);
            }
            curr_sender_idx = (curr_sender_idx + 1) % self.sender.buffer_size();
        }
        Ok(())
    }
}

pub fn run_intermediate_op(op: Box<dyn IntermediateOperator>, send_to: MultiSender) -> MultiSender {
    let (sender, receiver) = create_channel(*NUM_CPUS, send_to.in_order());
    let mut actor = IntermediateOpActor::new(op, receiver, send_to);
    tokio::spawn(async move {
        let _ = actor.run().await;
    });
    sender
}
