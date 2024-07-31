use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;


pub enum BlockingSinkStatus {
    NeedMoreInput,
    Finished
}


pub trait BlockingSink: Send + Sync {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus>;
    fn finalize(&mut self) -> DaftResult<()> {
        Ok(())
    }
    fn name(&self) -> &'static str;
}