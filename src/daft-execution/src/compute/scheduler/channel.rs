use async_trait::async_trait;
use common_error::DaftResult;

use crate::compute::partition::PartitionRef;

#[async_trait(?Send)]
pub trait OutputChannel<T: PartitionRef> {
    async fn send_output(&mut self, output: DaftResult<Vec<T>>) -> DaftResult<()>;
}
