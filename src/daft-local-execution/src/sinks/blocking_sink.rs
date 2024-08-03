use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::sources::source::Source;

pub enum BlockingSinkStatus {
    NeedMoreInput,
    #[allow(dead_code)]
    Finished,
}

pub trait BlockingSink: Send + Sync {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus>;
    fn finalize(&mut self) -> DaftResult<()> {
        Ok(())
    }
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    fn as_source(&mut self) -> &mut dyn Source;
}
