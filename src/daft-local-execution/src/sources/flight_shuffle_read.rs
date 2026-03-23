use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{combine_stream, get_io_runtime};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use daft_shuffles::client::FlightClientManager;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use tracing::instrument;

use super::source::{Source, SourceStream};
use crate::{channel::Sender, pipeline::NodeName};

pub struct FlightShuffleReadSource {
    shuffle_id: u64,
    partition_idx: usize,
    server_cache_mapping: HashMap<String, Vec<u32>>,
    schema: SchemaRef,
}

impl FlightShuffleReadSource {
    pub fn new(
        shuffle_id: u64,
        partition_idx: usize,
        _server_addresses: Vec<String>,
        server_cache_mapping: HashMap<String, Vec<u32>>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            shuffle_id,
            partition_idx,
            server_cache_mapping,
            schema,
        }
    }

    fn spawn_flight_shuffle_reader(
        shuffle_id: u64,
        partition_idx: usize,
        server_cache_mapping: HashMap<String, Vec<u32>>,
        schema: SchemaRef,
        output_sender: Sender<Arc<MicroPartition>>,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);
        io_runtime.spawn(async move {
            let mut client_manager = FlightClientManager::new();
            let stream = client_manager
                .fetch_partition(
                    shuffle_id,
                    partition_idx,
                    &server_cache_mapping,
                    schema.clone(),
                )
                .await?;

            forward_partition_stream(stream, schema, output_sender).await
        })
    }
}

async fn forward_partition_stream(
    mut stream: BoxStream<'static, DaftResult<daft_recordbatch::RecordBatch>>,
    schema: SchemaRef,
    sender: Sender<Arc<MicroPartition>>,
) -> DaftResult<()> {
    while let Some(batch) = stream.next().await {
        let mp = MicroPartition::new_loaded(schema.clone(), vec![batch?].into(), None);
        if sender.send(Arc::new(mp)).await.is_err() {
            break;
        }
    }
    Ok(())
}

#[async_trait]
impl Source for FlightShuffleReadSource {
    fn name(&self) -> NodeName {
        "FlightShuffleRead".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "FlightShuffleRead: shuffle_id={}, partition_idx={}",
            self.shuffle_id, self.partition_idx
        )]
    }

    #[instrument(skip_all, name = "FlightShuffleReadSource::get_data")]
    fn get_data(
        &mut self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
        _chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) =
            crate::channel::create_channel::<Arc<MicroPartition>>(1);

        let processor_task = Self::spawn_flight_shuffle_reader(
            self.shuffle_id,
            self.partition_idx,
            std::mem::take(&mut self.server_cache_mapping),
            self.schema.clone(),
            output_sender,
        );

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }
}
