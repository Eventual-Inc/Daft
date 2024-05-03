pub mod exchange;
pub mod planner;
pub mod run;
pub mod runner;
pub mod sink;

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
    compute::partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    executor::Executor,
};

use exchange::Exchange;
use sink::SinkSpec;

static STAGE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct ExchangeStage<T: PartitionRef> {
    pub op: Box<dyn Exchange<T>>,
    pub inputs: Vec<VirtualPartitionSet<T>>,
    pub stage_id: usize,
}

impl<T: PartitionRef> ExchangeStage<T> {
    pub fn new(op: Box<dyn Exchange<T>>, inputs: Vec<VirtualPartitionSet<T>>) -> Self {
        let stage_id = STAGE_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self {
            op,
            inputs,
            stage_id,
        }
    }
}

pub struct SinkStage<T: PartitionRef, E: Executor<T> + 'static> {
    pub op: Box<dyn SinkSpec<T, E> + Send>,
    pub inputs: Vec<VirtualPartitionSet<T>>,
    pub stage_id: usize,
}

impl<T: PartitionRef, E: Executor<T> + 'static> SinkStage<T, E> {
    pub fn new(op: Box<dyn SinkSpec<T, E> + Send>, inputs: Vec<VirtualPartitionSet<T>>) -> Self {
        let stage_id = STAGE_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        Self {
            op,
            inputs,
            stage_id,
        }
    }

    pub fn buffer_size(&self) -> usize {
        self.op.buffer_size()
    }
}

pub enum Stage<T: PartitionRef, E: Executor<T> + 'static> {
    Exchange(ExchangeStage<T>),
    Sink(SinkStage<T, E>),
}

impl<T: PartitionRef, E: Executor<T> + 'static>
    From<(Box<dyn Exchange<T>>, Vec<VirtualPartitionSet<T>>)> for Stage<T, E>
{
    fn from(value: (Box<dyn Exchange<T>>, Vec<VirtualPartitionSet<T>>)) -> Self {
        let (op, inputs) = value;
        Self::Exchange(ExchangeStage::new(op, inputs))
    }
}

impl<T: PartitionRef, E: Executor<T> + 'static>
    From<(Box<dyn SinkSpec<T, E> + Send>, Vec<VirtualPartitionSet<T>>)> for Stage<T, E>
{
    fn from(value: (Box<dyn SinkSpec<T, E> + Send>, Vec<VirtualPartitionSet<T>>)) -> Self {
        let (op, inputs) = value;
        Self::Sink(SinkStage::new(op, inputs))
    }
}
