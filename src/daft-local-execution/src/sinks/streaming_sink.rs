use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

pub enum StreamSinkOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    #[allow(dead_code)]
    HasMoreOutput(Arc<MicroPartition>),
    Finished(Option<Arc<MicroPartition>>),
}

pub trait StreamingSink: Send + Sync {
    fn execute(
        &mut self,
        index: usize,
        input: &Arc<MicroPartition>,
    ) -> DaftResult<StreamSinkOutput>;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}
