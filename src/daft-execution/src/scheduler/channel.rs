use async_trait::async_trait;
use common_error::DaftResult;

use crate::partition::PartitionRef;

/// Interface for an output channel that a partition task scheduler can send final outputs to.
#[async_trait(?Send)]
pub trait OutputChannel<T: PartitionRef> {
    async fn send_output(&mut self, output: DaftResult<Vec<T>>) -> DaftResult<()>;
}
