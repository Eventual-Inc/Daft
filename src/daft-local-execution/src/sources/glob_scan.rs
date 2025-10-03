use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use common_io_config::IOConfig;
use common_metrics::ops::NodeType;
use common_runtime::get_io_runtime;
use common_scan_info::Pushdowns;
use daft_core::prelude::*;
use daft_io::{IOStatsRef, get_io_client};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use log::warn;
use tracing::instrument;

use super::source::Source;
use crate::{channel::create_channel, pipeline::NodeName, sources::source::SourceStream};

#[allow(dead_code)]
pub struct GlobScanSource {
    glob_paths: Arc<Vec<String>>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    io_config: Option<IOConfig>,
}

impl GlobScanSource {
    pub fn new(
        glob_paths: Arc<Vec<String>>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            glob_paths,
            pushdowns,
            schema,
            io_config,
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for GlobScanSource {
    #[instrument(name = "GlobScanSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        // Get IO client
        let io_config = self.io_config.clone().unwrap_or_default();
        let io_client = get_io_client(true, Arc::new(io_config))?;
        let io_runtime = get_io_runtime(true);
        let schema = self.schema.clone();
        let glob_paths = self.glob_paths.clone();
        let limit = self.pushdowns.limit;

        let (tx, rx) = create_channel(0);
        // Create a stream that processes files in chunks
        let task = io_runtime
            .spawn(async move {
                let io_client = io_client.clone();
                let io_stats = io_stats.clone();

                let mut seen_paths = HashSet::new();
                for glob_path in glob_paths.iter().unique() {
                    let (source, path) = io_client.get_source_and_path(glob_path).await?;
                    let io_stats = io_stats.clone();
                    let schema = schema.clone();

                    let stream = source
                        .glob(
                            &path,
                            None,  // fanout_limit
                            None,  // page_size
                            limit, // limit
                            Some(io_stats),
                            None, // file_format
                        )
                        .await?
                        .chunks(chunk_size)
                        .map(|files_chunk| {
                            let mut paths = Vec::with_capacity(files_chunk.len());
                            let mut sizes = Vec::with_capacity(files_chunk.len());

                            for file_result in files_chunk {
                                match file_result {
                                    Ok(file_metadata) => {
                                        if seen_paths.contains(&file_metadata.filepath) {
                                            continue;
                                        }
                                        seen_paths.insert(file_metadata.filepath.clone());
                                        paths.push(file_metadata.filepath.clone());
                                        sizes.push(file_metadata.size.map(|s| s as i64));
                                    }
                                    Err(daft_io::Error::NotFound { path, .. }) => {
                                        warn!("File not found: {}", path);
                                    }
                                    Err(e) => return Err(e.into()),
                                }
                            }

                            let num_rows = paths.len();
                            let path_array =
                                Utf8Array::from_values("path", paths.into_iter()).into_series();
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

                    let mut remaining_rows = limit;
                    let mut stream =
                        stream.try_take_while(move |partition| match (partition, remaining_rows) {
                            // Limit has been met, early-terminate.
                            (_, Some(0)) => futures::future::ready(Ok(false)),
                            // Limit has not yet been met, update remaining remaining_rows and continue.
                            (table, Some(rows_left)) => {
                                remaining_rows = Some(rows_left.saturating_sub(table.len()));
                                futures::future::ready(Ok(true))
                            }
                            // No limit, never early-terminate.
                            (_, None) => futures::future::ready(Ok(true)),
                        });
                    while let Some(batch) = stream.next().await {
                        if tx.send(batch).await.is_err() {
                            break;
                        }
                    }
                }
                if seen_paths.is_empty() {
                    return Err(common_error::DaftError::FileNotFound {
                        path: glob_paths.join(","),
                        source: "No files found".into(),
                    });
                }
                Ok(())
            })
            .map(|x| x?);

        let receiver_stream = rx.into_stream().boxed();
        let combined_stream = common_runtime::combine_stream(receiver_stream, task);
        Ok(combined_stream.boxed())
    }

    fn name(&self) -> NodeName {
        "GlobScanSource".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::GlobScan
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("GlobScanSource:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Glob paths = {:?}", self.glob_paths));
        if let Some(io_config) = &self.io_config {
            res.push(format!("IO Config = {:?}", io_config));
        }
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
