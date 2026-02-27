use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use daft_io::{IOClient, IOStatsRef};
use futures::{FutureExt, future::BoxFuture};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};

use crate::read_planner::{CoalescePass, ReadPlanner, SplitLargeRequestPass};

// IO coalescing/splitting constants — these match the parquet2 reader in file.rs:384-391
// so both paths have identical IO behavior.

/// Maximum hole size for the coalesce pass (1 MB).
/// Two byte ranges within this distance are merged into a single request,
/// trading a small amount of extra bandwidth for fewer round-trips.
const COALESCE_MAX_HOLE_SIZE: usize = 1024 * 1024;

/// Maximum request size for the coalesce pass (16 MB).
/// Caps how large a coalesced request can grow.
const COALESCE_MAX_REQUEST_SIZE: usize = 16 * 1024 * 1024;

/// Maximum chunk size when splitting oversized requests (16 MB).
const SPLIT_MAX_REQUEST_SIZE: usize = 16 * 1024 * 1024;

/// Requests larger than this threshold are split into SPLIT_MAX_REQUEST_SIZE chunks (24 MB).
const SPLIT_THRESHOLD: usize = 24 * 1024 * 1024;

// Footer constants — defined by the Parquet spec.

/// Parquet footer magic bytes: PAR1
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// Footer size: 4 bytes metadata length + 4 bytes magic.
const FOOTER_SIZE: usize = 8;

/// Speculative read size from end-of-file to capture the footer in one round-trip.
/// Matches the arrow-rs default. Most parquet footers are well under 64 KB;
/// if the footer is larger, a second fetch retrieves the remainder.
const DEFAULT_FOOTER_READ_SIZE: usize = 64 * 1024;

/// An implementation of the arrow-rs [`AsyncFileReader`] trait that uses Daft's
/// [`IOClient`] and [`ReadPlanner`] for fetching byte ranges.
///
/// This bridges Daft's I/O layer with the arrow-rs parquet reader, enabling
/// efficient coalesced and split I/O when reading parquet files from any
/// source that Daft supports (local, S3, GCS, Azure, HTTP, etc.).
pub struct DaftAsyncFileReader {
    uri: String,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    metadata: Option<Arc<ParquetMetaData>>,
    file_size: Option<usize>,
}

impl DaftAsyncFileReader {
    /// Create a new `DaftAsyncFileReader`.
    ///
    /// # Arguments
    /// * `uri` - The URI of the parquet file.
    /// * `io_client` - The Daft IOClient for performing I/O.
    /// * `io_stats` - Optional I/O statistics tracker.
    /// * `metadata` - Optional pre-cached parquet metadata.
    /// * `file_size` - Optional known file size (avoids a HEAD request when fetching the footer).
    pub fn new(
        uri: String,
        io_client: Arc<IOClient>,
        io_stats: Option<IOStatsRef>,
        metadata: Option<Arc<ParquetMetaData>>,
        file_size: Option<usize>,
    ) -> Self {
        Self {
            uri,
            io_client,
            io_stats,
            metadata,
            file_size,
        }
    }

    /// Helper: fetch a single byte range from the file as `Bytes`.
    async fn fetch_range(
        io_client: &Arc<IOClient>,
        uri: &str,
        range: Range<usize>,
        io_stats: Option<&IOStatsRef>,
    ) -> Result<Bytes, ParquetError> {
        let get_result = io_client
            .single_url_get(
                uri.to_string(),
                Some(daft_io::range::GetRange::Bounded(range)),
                io_stats.cloned(),
            )
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        get_result
            .bytes()
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))
    }
}

impl AsyncFileReader for DaftAsyncFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        let io_client = self.io_client.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();
        let usize_range = range.start as usize..range.end as usize;

        async move { Self::fetch_range(&io_client, &uri, usize_range, io_stats.as_ref()).await }
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        let io_client = self.io_client.clone();
        let uri = self.uri.clone();
        let io_stats = self.io_stats.clone();

        async move {
            if ranges.is_empty() {
                return Ok(vec![]);
            }

            // Convert u64 ranges to usize ranges for the ReadPlanner.
            let usize_ranges: Vec<Range<usize>> = ranges
                .iter()
                .map(|r| r.start as usize..r.end as usize)
                .collect();

            // Build a ReadPlanner with coalescing and splitting passes.
            let mut planner = ReadPlanner::new(&uri);
            for range in &usize_ranges {
                planner.add_range(range.start, range.end);
            }
            planner.add_pass(Box::new(CoalescePass {
                max_hole_size: COALESCE_MAX_HOLE_SIZE,
                max_request_size: COALESCE_MAX_REQUEST_SIZE,
            }));
            planner.add_pass(Box::new(SplitLargeRequestPass {
                max_request_size: SPLIT_MAX_REQUEST_SIZE,
                split_threshold: SPLIT_THRESHOLD,
            }));
            planner
                .run_passes()
                .map_err(|e| ParquetError::External(Box::new(e)))?;

            let ranges_container = planner
                .collect(io_client, io_stats)
                .map_err(|e| ParquetError::External(Box::new(e)))?;

            // For each originally requested range, extract the bytes from the
            // coalesced/split container.
            let mut results = Vec::with_capacity(usize_ranges.len());
            for range in &usize_ranges {
                let bytes = ranges_container
                    .get_range_bytes(range.clone())
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
                results.push(bytes);
            }

            Ok(results)
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, Result<Arc<ParquetMetaData>, ParquetError>> {
        async move {
            // Return cached metadata if available.
            if let Some(ref metadata) = self.metadata {
                return Ok(metadata.clone());
            }

            let io_client = self.io_client.clone();
            let uri = self.uri.clone();
            let io_stats = self.io_stats.clone();

            // Determine file size if we don't already know it.
            let file_size = match self.file_size {
                Some(size) => size,
                None => {
                    let size = io_client
                        .single_url_get_size(uri.clone(), io_stats.clone())
                        .await
                        .map_err(|e| ParquetError::External(Box::new(e)))?;
                    self.file_size = Some(size);
                    size
                }
            };

            if file_size < FOOTER_SIZE + 4 {
                // Minimum valid parquet: 4 bytes magic (header) + 4 bytes metadata_len + 4 bytes magic (footer) = 12
                return Err(ParquetError::General(format!(
                    "File too small to be a valid Parquet file: {} bytes (need at least 12)",
                    file_size
                )));
            }

            // Read the last N bytes (or the whole file if it's small).
            let footer_read_size = DEFAULT_FOOTER_READ_SIZE.min(file_size);
            let footer_start = file_size - footer_read_size;
            let data =
                Self::fetch_range(&io_client, &uri, footer_start..file_size, io_stats.as_ref())
                    .await?;

            // Validate magic bytes at the end.
            if data.len() < FOOTER_SIZE {
                return Err(ParquetError::General(format!(
                    "Footer read returned {} bytes, expected at least {}",
                    data.len(),
                    FOOTER_SIZE
                )));
            }
            let magic = &data[data.len() - 4..];
            if magic != PARQUET_MAGIC {
                return Err(ParquetError::General(format!(
                    "Invalid Parquet file '{}': footer magic is {:?}, expected {:?}",
                    uri, magic, PARQUET_MAGIC
                )));
            }

            // Parse the metadata length (4 bytes, little-endian, just before the magic).
            let metadata_len = i32::from_le_bytes(
                data[data.len() - 8..data.len() - 4]
                    .try_into()
                    .expect("slice is exactly 4 bytes"),
            );
            if metadata_len < 0 {
                return Err(ParquetError::General(format!(
                    "Invalid Parquet metadata length: {}",
                    metadata_len
                )));
            }
            let metadata_len = metadata_len as usize;
            let footer_total = FOOTER_SIZE + metadata_len;

            if footer_total > file_size {
                return Err(ParquetError::General(format!(
                    "Parquet footer size ({}) exceeds file size ({})",
                    footer_total, file_size
                )));
            }

            // If our initial read didn't capture the full metadata, fetch the remainder.
            let metadata_bytes = if footer_total <= data.len() {
                // The full metadata + footer is within what we already read.
                let offset = data.len() - footer_total;
                data.slice(offset..offset + metadata_len)
            } else {
                // Need to fetch more data from the file.
                let metadata_start = file_size - footer_total;
                let metadata_end = file_size - FOOTER_SIZE;
                Self::fetch_range(
                    &io_client,
                    &uri,
                    metadata_start..metadata_end,
                    io_stats.as_ref(),
                )
                .await?
            };

            // Decode the thrift-encoded FileMetaData.
            let metadata =
                parquet::file::metadata::ParquetMetaDataReader::decode_metadata(&metadata_bytes)?;

            let metadata = Arc::new(metadata);
            self.metadata = Some(metadata.clone());
            Ok(metadata)
        }
        .boxed()
    }
}
