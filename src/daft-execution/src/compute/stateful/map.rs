use futures::Stream;

use crate::compute::partition::PartitionRef;

pub trait StatefulMap<T: PartitionRef> {
    fn run(&self, inputs: Vec<Box<dyn Stream<Item = T>>>) -> Vec<impl Stream<Item = T>>;
}
