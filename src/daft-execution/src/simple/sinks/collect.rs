use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::simple::common::{Sink, SinkResultType};

pub struct CollectSink {
    partitions: Vec<Arc<MicroPartition>>,
}

impl CollectSink {
    pub fn new() -> Self {
        Self { partitions: vec![] }
    }
}

impl Sink for CollectSink {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.partitions.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        Ok(self.partitions.clone())
    }
}
