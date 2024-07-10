use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::Stream;

#[async_trait]
pub trait Source: Send + Sync {
    async fn get_data(
        &self,
    ) -> Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send + Unpin>;
}
