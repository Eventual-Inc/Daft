use std::sync::Arc;

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_io_config::IOConfig;
use common_metrics::ops::NodeType;
use common_runtime::{combine_stream, get_io_runtime};
use common_scan_info::Pushdowns;
use daft_core::prelude::*;
use daft_io::{IOStatsRef, get_io_client};
use daft_local_plan::InputId;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use dashmap::DashSet;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use log::warn;
use tokio::sync::Mutex as AsyncMutex;
use tracing::instrument;

use super::source::Source;
use crate::{
    channel::{Sender, UnboundedReceiver, create_channel},
    pipeline::NodeName,
    sources::source::SourceStream,
};

pub struct GlobScanSource {
    receiver: Option<UnboundedReceiver<(InputId, Vec<String>)>>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    io_config: Option<IOConfig>,
}

impl GlobScanSource {
    pub fn new(
        receiver: UnboundedReceiver<(InputId, Vec<String>)>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            receiver: Some(receiver),
            pushdowns,
            schema,
            io_config,
        }
    }

    /// Spawns the background task that continuously reads glob paths from receiver and processes them
    fn spawn_glob_path_processor(
        &self,
        mut receiver: UnboundedReceiver<(InputId, Vec<String>)>,
        output_sender: Sender<Arc<MicroPartition>>,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);
        let pushdowns = self.pushdowns.clone();
        let schema = self.schema.clone();
        let io_config = self.io_config.clone();

        io_runtime.spawn(async move {
            let io_client = get_io_client(true, Arc::new(io_config.unwrap_or_default()))?;
            while let Some((_input_id, glob_paths)) = receiver.recv().await {
                let remaining_rows = Arc::new(AsyncMutex::new(pushdowns.limit));
                let seen_paths = Arc::new(DashSet::new());

                // Iterate over the unique glob paths and stream out the record batches
                let tasks = glob_paths.iter().unique().map(|glob_path| {
                    let io_client = io_client.clone();
                    let schema = schema.clone();
                    let io_stats = io_stats.clone();
                    let output_sender = output_sender.clone();
                    let seen_paths = seen_paths.clone();
                    let remaining_rows = remaining_rows.clone();

                    async move {
                        let (source, path) = io_client.get_source_and_path(glob_path).await?;

                        let mut stream = source
                            .glob(
                                &path,
                                None, // fanout_limit
                                None, // page_size
                                None, // limit
                                Some(io_stats.clone()),
                                None, // file_format
                            )
                            .await?
                            .chunks(chunk_size)
                            .map(move |files_chunk| {
                                let mut paths = Vec::with_capacity(files_chunk.len());
                                let mut sizes = Vec::with_capacity(files_chunk.len());

                                for file_result in files_chunk {
                                    match file_result {
                                        Ok(file_metadata) => {
                                            let filepath = file_metadata.filepath;
                                            if seen_paths.insert(filepath.clone()) {
                                                paths.push(filepath.clone());
                                                sizes.push(file_metadata.size.map(|s| s as i64));
                                            }
                                        }
                                        Err(daft_io::Error::NotFound { path, .. }) => {
                                            warn!("File not found: {}", path);
                                        }
                                        Err(e) => return Err(DaftError::from(e)),
                                    }
                                }

                                let num_rows = paths.len();
                                let path_array =
                                    Utf8Array::from_slice("path", &paths).into_series();
                                let size_array = Int64Array::from_iter(
                                    Field::new("size", DataType::Int64),
                                    sizes.into_iter(),
                                )
                                .into_series();
                                let rows_array =
                                    Int64Array::full_null("num_rows", &DataType::Int64, num_rows)
                                        .into_series();

                                let record_batch = RecordBatch::new_unchecked(
                                    schema.clone(),
                                    vec![path_array, size_array, rows_array],
                                    num_rows,
                                );
                                Ok(Arc::new(MicroPartition::new_loaded(
                                    schema.clone(),
                                    Arc::new(vec![record_batch]),
                                    None,
                                )))
                            });

                        while let Some(result) = stream.next().await {
                            let partition = result?;

                            {
                                let mut remaining = remaining_rows.lock().await;
                                match *remaining {
                                    // Limit has been met, early terminate.
                                    Some(0) => break,
                                    // Limit has not yet been met, update remaining_rows and continue.
                                    Some(rows_left) => {
                                        let new_remaining =
                                            rows_left.saturating_sub(partition.len());
                                        *remaining = Some(new_remaining);
                                    }
                                    // No limit, never early-terminate.
                                    None => {}
                                }
                            }

                            if output_sender.send(partition).await.is_err() {
                                break;
                            }
                        }

                        Ok::<(), DaftError>(())
                    }
                });

                futures::future::try_join_all(tasks).await?;

                // No files were matched across all paths
                if seen_paths.is_empty() {
                    warn!(
                        "No matching file found by glob paths: '{}'",
                        glob_paths.join(", ")
                    );
                }
            }
            Ok(())
        })
    }
}

#[async_trait]
impl Source for GlobScanSource {
    #[instrument(name = "GlobScanSource::get_data", level = "info", skip_all)]
    fn get_data(
        &mut self,
        _maintain_order: bool,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let (output_sender, output_receiver) = create_channel::<Arc<MicroPartition>>(1);
        let input_reiver = self.receiver.take().expect("Receiver not found");

        let processor_task =
            self.spawn_glob_path_processor(input_reiver, output_sender, io_stats, chunk_size);

        let result_stream = output_receiver.into_stream().map(Ok);
        let combined_stream = combine_stream(result_stream, processor_task.map(|x| x?));
        Ok(Box::pin(combined_stream))
    }

    fn name(&self) -> NodeName {
        "Glob Scan".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::GlobScan
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("Glob Scan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        if let Some(io_config) = &self.io_config {
            res.push(format!("IO Config = {:?}", io_config));
        }
        res.extend(self.pushdowns.multiline_display());
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
