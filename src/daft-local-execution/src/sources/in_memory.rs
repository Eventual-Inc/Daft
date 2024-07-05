use std::{pin::Pin, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{stream, Stream};

use super::source::Source;

#[derive(Clone)]
pub struct InMemorySource {
    data: Vec<Arc<MicroPartition>>,
}

impl InMemorySource {
    pub fn new(data: Vec<Arc<MicroPartition>>) -> Self {
        Self { data }
    }
}

impl Source for InMemorySource {
    fn get_data(&self) -> Pin<Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
        println!("InMemorySource::get_data");
        Box::pin(stream::iter(self.data.clone().into_iter().map(Ok)))
    }
}
