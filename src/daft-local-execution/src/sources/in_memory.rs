use std::sync::Arc;

use async_trait::async_trait;
use daft_micropartition::MicroPartition;
use futures::{stream, StreamExt};

use super::source::{Source, SourceStream};

pub struct InMemorySource {
    data: Vec<Arc<MicroPartition>>,
}

impl InMemorySource {
    pub fn new(data: Vec<Arc<MicroPartition>>) -> Self {
        Self { data }
    }
}

#[async_trait]
impl Source for InMemorySource {
    async fn get_data(&self) -> SourceStream {
        log::debug!("InMemorySource::get_data");
        stream::iter(self.data.clone().into_iter().map(Ok)).boxed()
    }
}
