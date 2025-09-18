use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use common_io_config::IOConfig;
use common_scan_info::Pushdowns;
use daft_core::prelude::*;
use daft_io::{IOStatsRef, get_io_client};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use tracing::instrument;

use super::source::Source;
use crate::{ops::NodeType, pipeline::NodeName, sources::source::SourceStream};

pub struct GlobScanSource {
    glob_paths: Vec<String>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    io_config: Option<IOConfig>,
}

impl GlobScanSource {
    pub fn new(
        glob_paths: Vec<String>,
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
        chunk_size: Option<usize>,
    ) -> DaftResult<SourceStream<'static>> {
        // Get IO client
        let io_config = self.io_config.clone().unwrap_or_default();
        let io_client = get_io_client(true, Arc::new(io_config))?;
        let schema = self.schema.clone();
        let glob_paths = self.glob_paths.clone();
        let chunk_size = chunk_size.unwrap_or(1000); // Default chunk size

        // Create a stream that processes files in chunks
        let stream = futures::stream::iter(glob_paths.into_iter())
            .then(move |glob_path| {
                let io_client = io_client.clone();
                let schema = schema.clone();
                let io_stats = io_stats.clone();
                let chunk_size = chunk_size;

                async move {
                    let (source, path) = io_client.get_source_and_path(&glob_path).await?;
                    let files_stream = source
                        .glob(
                            &path,
                            None, // fanout_limit
                            None, // page_size
                            None, // limit
                            Some(io_stats),
                            None, // file_format
                        )
                        .await?;

                    // Process files in chunks, handling Results properly
                    let chunked_stream = files_stream.chunks(chunk_size).map(move |files_chunk| {
                        let schema = schema.clone();

                        // Handle Results in the chunk
                        let mut paths = Vec::new();
                        let mut sizes = Vec::new();
                        let mut rows = Vec::new();

                        for file_result in files_chunk {
                            match file_result {
                                Ok(file_metadata) => {
                                    paths.push(file_metadata.filepath.clone());
                                    sizes.push(file_metadata.size.map(|s| s as i64));
                                    rows.push(0); // TODO: Calculate actual row counts if needed
                                }
                                Err(e) => return Err(e.into()),
                            }
                        }

                        // Create arrays
                        let path_array = Utf8Array::from_iter("path", paths.into_iter().map(Some));
                        let size_array = Int64Array::from_iter(
                            Field::new("size", DataType::Int64),
                            sizes.into_iter(),
                        );
                        let rows_array = Int64Array::from_iter(
                            Field::new("rows", DataType::Int64),
                            rows.into_iter().map(Some),
                        );

                        // Create record batch
                        let arrays = vec![
                            path_array.as_arrow().clone().boxed(),
                            size_array.as_arrow().clone().boxed(),
                            rows_array.as_arrow().clone().boxed(),
                        ];
                        let record_batch = RecordBatch::from_arrow(schema.clone(), arrays)?;

                        // Create MicroPartition for this chunk
                        let micro_partition =
                            MicroPartition::new_loaded(schema, Arc::new(vec![record_batch]), None);

                        Ok(Arc::new(micro_partition))
                    });

                    Ok::<_, common_error::DaftError>(chunked_stream)
                }
            })
            .try_flatten();

        Ok(Box::pin(stream))
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
