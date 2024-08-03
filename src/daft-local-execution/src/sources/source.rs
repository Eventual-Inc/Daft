use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::stream::BoxStream;

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

pub trait Source: Send + Sync {
    fn get_data(&self, maintain_order: bool) -> SourceStream;
}
