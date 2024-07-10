use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::stream::BoxStream;

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

#[async_trait]
pub trait Source: Send + Sync {
    async fn get_data(&self) -> SourceStream;
}
