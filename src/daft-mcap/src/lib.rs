//! Native MCAP scan support.
//!
//! This crate turns one MCAP file into a stream of Daft record batches:
//!
//! - [`stream_mcap`] is the scan entry point. It applies topic/time
//!   constraints while decoding, then residual predicates, projection, and
//!   an exact limit.
//! - [`read`] holds [`NativeMcapReader`]: indexed chunk traversal with a
//!   linear fallback.
//!
//! Peak memory scales with chunk size and batch payload size, not total file
//! size; a writer module may join `read` later.

use std::{io::SeekFrom, sync::Arc};

use bytes::Bytes;
use common_error::{DaftError, DaftResult};
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_io::{GetRange, GetResult, IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use mcap::sans_io::{SummaryReadEvent, SummaryReader, SummaryReaderOptions};

mod pushdown;
mod read;
#[cfg(test)]
mod test_utils;

pub use read::NativeMcapReader;

pub(crate) const MAX_RECORD_LENGTH: usize = 1024 * 1024 * 1024;
const SUMMARY_READ_AHEAD_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn mcap_error(error: impl std::fmt::Display) -> DaftError {
    DaftError::ComputeError(format!("Failed to read MCAP messages: {error}"))
}

fn invalid_seek_error(position: SeekFrom, file_size: u64) -> DaftError {
    mcap_error(format!(
        "invalid summary seek {position:?} for file of {file_size} bytes"
    ))
}

fn seek_position(position: SeekFrom, current: u64, file_size: u64) -> DaftResult<u64> {
    let value = match position {
        SeekFrom::Start(offset) => i128::from(offset),
        SeekFrom::Current(offset) => i128::from(current) + i128::from(offset),
        SeekFrom::End(offset) => i128::from(file_size) + i128::from(offset),
    };
    if !(0..=i128::from(u64::MAX)).contains(&value) {
        return Err(invalid_seek_error(position, file_size));
    }
    Ok(value as u64)
}

async fn fetch_range(
    uri: &str,
    start: u64,
    length: usize,
    io_client: &Arc<IOClient>,
    io_stats: &IOStatsRef,
) -> DaftResult<Bytes> {
    if length == 0 {
        return Ok(Bytes::new());
    }
    let start =
        usize::try_from(start).map_err(|_| mcap_error("range offset does not fit in usize"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| mcap_error("range end overflow"))?;
    let result = io_client
        .single_url_get(
            uri.to_string(),
            Some(GetRange::Bounded(start..end)),
            Some(io_stats.clone()),
        )
        .await?;
    let local = matches!(&result, GetResult::File(_));
    let bytes = result.bytes().await?;
    if local {
        io_stats.mark_bytes_read(bytes.len());
    }
    Ok(bytes)
}

pub(crate) async fn fetch_exact_range(
    uri: &str,
    start: u64,
    length: usize,
    io_client: &Arc<IOClient>,
    io_stats: &IOStatsRef,
) -> DaftResult<Bytes> {
    let bytes = fetch_range(uri, start, length, io_client, io_stats).await?;
    if bytes.len() != length {
        return Err(mcap_error(format!(
            "unexpected EOF reading range at {start}: requested {length} bytes, received {}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

pub(crate) struct BufferedRange {
    pub(crate) start: u64,
    pub(crate) bytes: Bytes,
}

impl BufferedRange {
    pub(crate) fn slice(&self, start: u64, length: usize) -> Option<Bytes> {
        let relative_start = usize::try_from(start.checked_sub(self.start)?).ok()?;
        let relative_end = relative_start.checked_add(length)?;
        (relative_end <= self.bytes.len()).then(|| self.bytes.slice(relative_start..relative_end))
    }
}

/// Reads the optional summary section from the end of an MCAP file.
#[tracing::instrument(
    skip_all,
    name = "McapReader::read_summary",
    fields(file_size = file_size, is_remote = is_remote),
    err
)]
pub(crate) async fn read_summary(
    uri: &str,
    file_size: usize,
    is_remote: bool,
    io_client: &Arc<IOClient>,
    io_stats: &IOStatsRef,
) -> DaftResult<Option<mcap::Summary>> {
    let file_size_u64 =
        u64::try_from(file_size).map_err(|_| mcap_error("file size does not fit in u64"))?;
    let mut reader = SummaryReader::new_with_options(
        SummaryReaderOptions::default()
            .with_file_size(file_size_u64)
            .with_record_length_limit(MAX_RECORD_LENGTH),
    );
    let mut position = 0_u64;
    let mut read_buffer: Option<BufferedRange> = None;

    while let Some(event) = reader.next_event() {
        match event.map_err(mcap_error)? {
            SummaryReadEvent::SeekRequest(request) => {
                position = seek_position(request, position, file_size_u64)?;
                reader.notify_seeked(position);
            }
            SummaryReadEvent::ReadRequest(requested) => {
                let remaining = file_size_u64.saturating_sub(position);
                let available = requested.min(usize::try_from(remaining).unwrap_or(usize::MAX));
                let bytes = if available == 0 {
                    Bytes::new()
                } else if !is_remote {
                    fetch_range(uri, position, available, io_client, io_stats).await?
                } else if let Some(bytes) = read_buffer
                    .as_ref()
                    .and_then(|buffer| buffer.slice(position, available))
                {
                    bytes
                } else {
                    let read_length = available
                        .max(SUMMARY_READ_AHEAD_BYTES)
                        .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                    let buffer = BufferedRange {
                        start: position,
                        bytes: fetch_exact_range(uri, position, read_length, io_client, io_stats)
                            .await?,
                    };
                    let bytes = buffer.slice(position, available).ok_or_else(|| {
                        DaftError::InternalError(
                            "MCAP summary read-ahead did not contain requested bytes".to_string(),
                        )
                    })?;
                    read_buffer = Some(buffer);
                    bytes
                };
                let read = bytes.len().min(requested);
                reader.insert(requested)[..read].copy_from_slice(&bytes[..read]);
                reader.notify_read(read);
                position = position.saturating_add(read as u64);
            }
        }
    }

    Ok(reader.finish())
}

/// Parses the optional summary section from a fully buffered MCAP file.
pub(crate) fn parse_summary_from_bytes(bytes: &Bytes) -> DaftResult<Option<mcap::Summary>> {
    let file_size = bytes.len() as u64;
    let mut reader = SummaryReader::new_with_options(
        SummaryReaderOptions::default()
            .with_file_size(file_size)
            .with_record_length_limit(MAX_RECORD_LENGTH),
    );
    let mut position = 0_u64;
    while let Some(event) = reader.next_event() {
        match event.map_err(mcap_error)? {
            SummaryReadEvent::SeekRequest(request) => {
                position = seek_position(request, position, file_size)?;
                reader.notify_seeked(position);
            }
            SummaryReadEvent::ReadRequest(requested) => {
                let start = usize::try_from(position)
                    .map_err(|_| mcap_error("summary position does not fit in usize"))?;
                let available = bytes.len().saturating_sub(start).min(requested);
                reader.insert(requested)[..available]
                    .copy_from_slice(&bytes[start..start + available]);
                reader.notify_read(available);
                position = position.saturating_add(available as u64);
            }
        }
    }
    Ok(reader.finish())
}

#[derive(Clone, Debug)]
pub struct McapReadOptions {
    pub batch_size: usize,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub topics: Option<Vec<String>>,
}

impl Default for McapReadOptions {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            start_time: None,
            end_time: None,
            topics: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct McapConvertOptions {
    pub predicate: Option<ExprRef>,
    pub include_columns: Option<Vec<String>>,
    pub limit: Option<usize>,
}

fn reader_batch_stream(
    mut reader: NativeMcapReader,
    prefetch: bool,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    if !prefetch {
        return Box::pin(futures::stream::try_unfold(
            reader,
            |mut reader| async move {
                let batch = reader.next_batch().await?;
                Ok(batch.map(|batch| (batch, reader)))
            },
        ));
    }

    // Buffer one batch to overlap decoding while preserving backpressure.
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let runtime = tokio::runtime::Handle::current();
    let producer: tokio::task::JoinHandle<DaftResult<()>> =
        tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                while let Some(batch) = reader.next_batch().await? {
                    if sender.send(batch).await.is_err() {
                        break;
                    }
                }
                Ok(())
            })
        });

    Box::pin(futures::stream::try_unfold(
        (receiver, Some(producer)),
        |(mut receiver, mut producer)| async move {
            if let Some(batch) = receiver.recv().await {
                return Ok(Some((batch, (receiver, producer))));
            }
            if let Some(producer) = producer.take() {
                producer.await??;
            }
            Ok(None)
        },
    ))
}

/// Stream MCAP messages as record batches.
///
/// Topic/time constraints — explicit ones and any recovered from the
/// predicate — are applied while decoding. Residual predicates run before
/// projection so filters can reference unprojected columns. The final batch
/// is truncated to honor the limit exactly.
pub async fn stream_mcap(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: McapReadOptions,
    convert_options: McapConvertOptions,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let read_options =
        pushdown::apply_predicate_pushdown(read_options, convert_options.predicate.as_ref());
    let reader = NativeMcapReader::new(uri, io_client, io_stats, read_options).await?;
    let batches = reader_batch_stream(reader, convert_options.limit.is_none());
    let stream = futures::stream::try_unfold(
        (
            batches,
            convert_options.predicate,
            convert_options.include_columns,
            convert_options.limit,
        ),
        |(mut batches, predicate, include_columns, mut remaining_rows)| async move {
            if remaining_rows == Some(0) {
                return Ok(None);
            }

            loop {
                let Some(mut batch) = batches.next().await.transpose()? else {
                    return Ok(None);
                };

                if let Some(predicate) = &predicate {
                    let predicate = BoundExpr::try_new(predicate.clone(), &batch.schema)?;
                    batch = batch.filter(&[predicate])?;
                }
                if batch.is_empty() {
                    continue;
                }

                if let Some(include_columns) = &include_columns {
                    let include_column_indices = include_columns
                        .iter()
                        .map(|name| batch.schema.get_index(name))
                        .collect::<DaftResult<Vec<_>>>()?;
                    batch = batch.get_columns(&include_column_indices);
                }

                if let Some(rows_left) = remaining_rows {
                    let rows_to_emit = rows_left.min(batch.len());
                    batch = batch.head(rows_to_emit)?;
                    remaining_rows = Some(rows_left - rows_to_emit);
                }

                return Ok(Some((
                    batch,
                    (batches, predicate, include_columns, remaining_rows),
                )));
            }
        },
    );
    Ok(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_dsl::{lit, resolved_col};
    use tempfile::NamedTempFile;

    use super::{BufferedRange, McapConvertOptions, McapReadOptions};
    use crate::test_utils::{collect_stream, write_corrupt_chunk_mcap, write_mcap};

    #[test]
    fn buffered_range_slices_within_bounds() {
        let buffer = BufferedRange {
            start: 100,
            bytes: bytes::Bytes::from_static(b"abcdefgh"),
        };
        assert_eq!(buffer.slice(102, 3).as_deref(), Some(&b"cde"[..]));
        assert!(buffer.slice(99, 1).is_none());
        assert!(buffer.slice(107, 2).is_none());
    }

    #[tokio::test]
    async fn stream_filters_before_projection_and_applies_exact_limit() -> DaftResult<()> {
        let file = write_mcap(true, None, 20, 8);
        let batches = collect_stream(
            &file,
            McapReadOptions {
                batch_size: 4,
                ..Default::default()
            },
            McapConvertOptions {
                predicate: Some(resolved_col("sequence").gt(lit(5_u32))),
                include_columns: Some(vec!["topic".to_string()]),
                limit: Some(3),
            },
        )
        .await?;

        assert_eq!(batches.iter().map(|batch| batch.len()).sum::<usize>(), 3);
        assert!(batches.iter().all(|batch| batch.num_columns() == 1));
        let topics = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .get_column(0)
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .map(|value| value.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(topics, vec!["/camera", "/imu", "/camera"]);
        Ok(())
    }

    #[tokio::test]
    async fn stream_without_limit_preserves_all_rows() -> DaftResult<()> {
        let file = write_mcap(true, None, 20, 8);
        let batches = collect_stream(
            &file,
            McapReadOptions {
                batch_size: 4,
                ..Default::default()
            },
            McapConvertOptions::default(),
        )
        .await?;

        assert_eq!(batches.len(), 5);
        assert_eq!(batches.iter().map(|batch| batch.len()).sum::<usize>(), 20);
        Ok(())
    }

    #[tokio::test]
    async fn stream_pushes_predicate_constraints_into_index() -> DaftResult<()> {
        use std::sync::Arc;

        // 16 messages x 64 KiB: an index-pruned read of one message must
        // fetch well under half the file even with no explicit constraints.
        let file = write_mcap(true, None, 16, 64 * 1024);
        let file_size = file.as_file().metadata().unwrap().len() as usize;
        let predicate = resolved_col("topic")
            .eq(lit("/camera"))
            .and(resolved_col("log_time").gt_eq(lit(8_u64)))
            .and(resolved_col("log_time").lt(lit(9_u64)));

        let io_client = daft_io::get_io_client(true, Arc::new(daft_io::IOConfig::default()))?;
        let io_stats = daft_io::IOStatsContext::new("daft-mcap pushdown test");
        let uri = file.path().to_string_lossy().into_owned();
        let batches: Vec<_> = futures::TryStreamExt::try_collect(
            crate::stream_mcap(
                &uri,
                io_client,
                io_stats.clone(),
                McapReadOptions::default(),
                McapConvertOptions {
                    predicate: Some(predicate),
                    include_columns: None,
                    limit: None,
                },
            )
            .await?,
        )
        .await?;

        assert_eq!(batches.iter().map(|batch| batch.len()).sum::<usize>(), 1);
        assert!(
            io_stats.load_bytes_read() < file_size / 2,
            "predicate-pruned read fetched {} of {file_size} bytes",
            io_stats.load_bytes_read()
        );
        Ok(())
    }

    #[tokio::test]
    async fn stream_predicate_matches_explicit_kwargs() -> DaftResult<()> {
        fn log_times(batches: &[daft_recordbatch::RecordBatch]) -> Vec<u64> {
            batches
                .iter()
                .flat_map(|batch| {
                    let times = batch.get_column(2).u64().unwrap();
                    (0..batch.len())
                        .map(|index| times.get(index).unwrap())
                        .collect::<Vec<_>>()
                })
                .collect()
        }

        let file = write_mcap(true, None, 40, 32);
        let via_kwargs = collect_stream(
            &file,
            McapReadOptions {
                start_time: Some(4),
                end_time: Some(20),
                topics: Some(vec!["/camera".to_string()]),
                ..Default::default()
            },
            McapConvertOptions::default(),
        )
        .await?;
        let via_predicate = collect_stream(
            &file,
            McapReadOptions::default(),
            McapConvertOptions {
                predicate: Some(
                    resolved_col("topic")
                        .eq(lit("/camera"))
                        .and(resolved_col("log_time").gt_eq(lit(4_u64)))
                        .and(resolved_col("log_time").lt(lit(20_u64))),
                ),
                include_columns: None,
                limit: None,
            },
        )
        .await?;

        let expected = log_times(&via_kwargs);
        assert!(!expected.is_empty());
        assert_eq!(log_times(&via_predicate), expected);
        Ok(())
    }

    #[tokio::test]
    async fn prefetched_stream_propagates_reader_errors() -> DaftResult<()> {
        // A corrupt chunk fails mid-stream, after the reader opens cleanly,
        // so the error must travel through the prefetch channel.
        let file = write_corrupt_chunk_mcap();
        let error = collect_stream(
            &file,
            McapReadOptions::default(),
            McapConvertOptions::default(),
        )
        .await
        .expect_err("corrupt MCAP chunk should fail");

        assert!(error.to_string().contains("Failed to read MCAP messages"));
        Ok(())
    }

    #[tokio::test]
    async fn stream_surfaces_open_errors() -> DaftResult<()> {
        let file = NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"not an mcap file").unwrap();
        let error = collect_stream(
            &file,
            McapReadOptions::default(),
            McapConvertOptions::default(),
        )
        .await
        .expect_err("invalid MCAP should fail");

        assert!(error.to_string().to_lowercase().contains("magic"));
        Ok(())
    }
}
