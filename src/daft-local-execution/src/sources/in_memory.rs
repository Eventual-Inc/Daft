use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{stream, Stream};

use super::source::Source;

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
    async fn get_data(
        &self,
    ) -> Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send + Unpin> {
        log::debug!("InMemorySource::get_data");
        Box::new(stream::iter(self.data.clone().into_iter().map(Ok)))
    }
}
