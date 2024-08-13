use std::sync::Arc;

use crate::{
    channel::{create_channel, MultiReceiver},
    ExecutionRuntimeHandle,
};
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::source::Source;

pub struct InMemorySource {
    data: Vec<Arc<MicroPartition>>,
}

impl InMemorySource {
    pub fn new(data: Vec<Arc<MicroPartition>>) -> Self {
        Self { data }
    }
    pub fn boxed(self) -> Box<dyn Source> {
        Box::new(self) as Box<dyn Source>
    }
}

impl Source for InMemorySource {
    #[instrument(name = "InMemorySource::get_data", level = "info", skip_all)]
    fn get_data(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<MultiReceiver> {
        let data = self.data.clone();
        let (mut sender, receiver) = create_channel(data.len(), maintain_order);
        runtime_handle.spawn(async move {
            for part in data {
                let _ = sender.get_next_sender().send(part.into()).await;
            }
            Ok(())
        });
        Ok(receiver)
    }
}
