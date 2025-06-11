use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::{partitioning::PartitionSetRef, MicroPartition, MicroPartitionRef};
use futures::StreamExt;
use tracing::instrument;

use super::source::Source;
use crate::channel::{create_channel, Receiver, Sender};

pub struct InMemorySource {
    data: Option<PartitionSetRef<MicroPartitionRef>>,
    size_bytes: usize,
    schema: SchemaRef,
}

impl InMemorySource {
    pub fn new(
        data: Option<PartitionSetRef<MicroPartitionRef>>,
        schema: SchemaRef,
        size_bytes: usize,
    ) -> Self {
        Self {
            data,
            size_bytes,
            schema,
        }
    }
    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for InMemorySource {
    #[instrument(name = "InMemorySource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        tx: Sender<(usize, Receiver<Arc<MicroPartition>>)>,
    ) -> DaftResult<()> {
        let (partition_sender, partition_receiver) = create_channel(1);
        if tx.send((0, partition_receiver)).await.is_err() {
            return Ok(());
        }
        if let Some(data) = &self.data {
            let mut stream = data.clone().to_partition_stream();

            // Send the data through the channel
            while let Some(partition) = stream.next().await {
                if partition_sender.send(partition?).await.is_err() {
                    break;
                }
            }
        }
        Ok(())
    }
    fn name(&self) -> &'static str {
        "InMemorySource"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("InMemorySource:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.size_bytes));
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
