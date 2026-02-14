use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

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
use futures::{FutureExt, StreamExt, future::join_all};
use itertools::Itertools;
use log::warn;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::instrument;

use super::source::Source;
use crate::{pipeline::NodeName, sources::source::SourceStream};

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
        let io_config = self.io_config.clone().unwrap_or_default();
        let io_client = get_io_client(true, Arc::new(io_config))?;
        let io_runtime = get_io_runtime(true);
        let schema = self.schema.clone();
        let glob_paths = self.glob_paths.clone();
        let limit = self.pushdowns.limit;

        // Spawn a task to stream out the record batches from the glob paths
        let (tx, rx) = mpsc::channel::<DaftResult<Arc<MicroPartition>>>(1);
        let task = io_runtime
            .spawn(async move {
                let io_client = io_client.clone();
                let io_stats = io_stats.clone();

                // Shared state across all glob path scan tasks
                let seen_paths = Arc::new(Mutex::new(HashSet::new()));
                let remaining_rows = Arc::new(Mutex::new(limit));
                let has_results = Arc::new(AtomicBool::new(false));

                // Create a glob task for each path
                let subtasks = glob_paths.iter().unique().map(|glob_path| {
                    let io_client = io_client.clone();
                    let io_stats = io_stats.clone();
                    let schema = schema.clone();
                    let tx = tx.clone();
                    let seen_paths = seen_paths.clone();
                    let remaining_rows = remaining_rows.clone();
                    let has_results = has_results.clone();

                    async move {
                        let (source, path) = io_client.get_source_and_path(glob_path).await?;
                        let io_stats = io_stats.clone();
                        let schema = schema.clone();

                        let mut stream = source
                            .glob(
                                &path,
                                None, // fanout_limit
                                None, // page_size
                                None, // limit
                                Some(io_stats),
                                None, // file_format
                            )
                            .await?
                            .chunks(chunk_size);

                        while let Some(files_chunk) = stream.next().await {
                            // Check global limit before processing the chunk
                            {
                                let remaining = remaining_rows.lock().await;
                                if matches!(*remaining, Some(0)) {
                                    break;
                                }
                            }

                            let mut paths = Vec::with_capacity(files_chunk.len());
                            let mut sizes = Vec::with_capacity(files_chunk.len());
                            for file_result in files_chunk {
                                match file_result {
                                    Ok(file_metadata) => {
                                        let filepath = file_metadata.filepath;
                                        // Skip if we've seen this path before
                                        if !seen_paths.lock().await.insert(filepath.clone()) {
                                            continue;
                                        }

                                        has_results.store(true, Ordering::Relaxed);
                                        paths.push(filepath);
                                        sizes.push(file_metadata.size.map(|s| s as i64));
                                    }
                                    Err(daft_io::Error::NotFound { path, .. }) => {
                                        warn!("File not found: {}", path);
                                    }
                                    Err(e) => return Err(e.into()),
                                }
                            }

                            let num_rows = paths.len();
                            if num_rows == 0 {
                                continue;
                            }

                            let path_array = Utf8Array::from_slice("path", &paths).into_series();
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
                            let partition = Arc::new(MicroPartition::new_loaded(
                                schema.clone(),
                                Arc::new(vec![record_batch]),
                                None,
                            ));

                            // Enforce global limit by truncating partition before send
                            let mut remaining = remaining_rows.lock().await;
                            let (send, done, take) = match *remaining {
                                Some(0) => (false, true, None),
                                Some(rows) => {
                                    let take = rows.min(partition.len());
                                    *remaining = Some(rows - take);
                                    (take > 0, *remaining == Some(0), Some(take))
                                }
                                None => (true, false, None),
                            };
                            drop(remaining);

                            if send {
                                let to_send = match take {
                                    Some(rows) if rows < partition.len() => {
                                        Arc::new(partition.head(rows)?)
                                    }
                                    _ => partition,
                                };

                                if tx.send(Ok(to_send)).await.is_err() {
                                    break;
                                }
                            }

                            if done {
                                break;
                            }
                        }

                        Ok::<(), common_error::DaftError>(())
                    }
                });

                let results = join_all(subtasks).await;
                for result in results {
                    result?;
                }

                // No files were matched across all paths
                if !has_results.load(Ordering::Relaxed) {
                    warn!(
                        "No matching file found by glob paths: '{}'",
                        glob_paths.join(", ")
                    );
                }

                Ok(())
            })
            .map(|x| x?);

        let combined_stream = common_runtime::combine_stream(ReceiverStream::new(rx), task);
        Ok(combined_stream.boxed())
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
        if self.glob_paths.len() <= 1 {
            res.push(format!(
                "Glob path: {}",
                self.glob_paths.first().expect("Empty glob paths")
            ));
        } else {
            res.push("Glob paths: [".to_string());
            for path in self.glob_paths.iter() {
                res.push(format!("  {}", path));
            }
            res.push("]".to_string());
        }
        if let Some(io_config) = &self.io_config {
            res.push(format!("IO Config = {:?}", io_config));
        }
        res
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
