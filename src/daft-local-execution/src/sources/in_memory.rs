use std::sync::Arc;

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

impl Source for InMemorySource {
    fn get_data(&self) -> SourceStream {
        stream::iter(self.data.clone().into_iter().map(Ok)).boxed()
    }
}
