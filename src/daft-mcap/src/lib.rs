//! Native MCAP reader core.
//!
//! [`NativeMcapReader`] decodes one MCAP file into Daft record batches using
//! indexed chunk traversal when a usable summary is present and a linear
//! fallback otherwise.
//!
//! Indexed traversal buffers overlapping chunks for log-time ordering. HTTP
//! servers without range support require a full-file buffer.

use std::{io::SeekFrom, sync::Arc};

use bytes::Bytes;
use common_error::{DaftError, DaftResult};
use daft_io::{GetRange, GetResult, IOClient, IOStatsRef};
use mcap::sans_io::{SummaryReadEvent, SummaryReader, SummaryReaderOptions};

mod indexed;
mod read;
mod stats;
#[cfg(test)]
mod test_utils;

pub use read::NativeMcapReader;
pub use stats::McapReadStats;

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

pub(crate) async fn fetch_range(
    uri: &str,
    start: usize,
    length: usize,
    io_client: &Arc<IOClient>,
    io_stats: &IOStatsRef,
) -> DaftResult<Bytes> {
    if length == 0 {
        return Ok(Bytes::new());
    }
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

/// Require the range contract when callers rely on the requested interval.
/// An oversized response is not necessarily extra bytes at the
/// requested offset: a server ignoring Range can return the file from byte zero.
/// Without a verified response offset, truncating that body would be unsafe.
pub(crate) async fn fetch_exact_range(
    uri: &str,
    start: usize,
    length: usize,
    io_client: &Arc<IOClient>,
    io_stats: &IOStatsRef,
) -> DaftResult<Bytes> {
    let bytes = fetch_range(uri, start, length, io_client, io_stats).await?;
    if bytes.len() != length {
        return Err(mcap_error(format!(
            "unexpected range length at {start}: requested {length} bytes, received {}",
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
    file_size: u64,
    is_remote: bool,
    io_client: &Arc<IOClient>,
    io_stats: &IOStatsRef,
    stats: &mut McapReadStats,
) -> DaftResult<Option<mcap::Summary>> {
    let mut reader = SummaryReader::new_with_options(
        SummaryReaderOptions::default()
            .with_file_size(file_size)
            .with_record_length_limit(MAX_RECORD_LENGTH),
    );
    let mut position = 0_u64;
    let mut read_buffer: Option<BufferedRange> = None;

    while let Some(event) = reader.next_event() {
        match event.map_err(mcap_error)? {
            SummaryReadEvent::SeekRequest(request) => {
                stats.summary_seeks += 1;
                position = seek_position(request, position, file_size)?;
                if stats.summary_seeks == 2 {
                    stats.summary_tail_bytes = file_size.checked_sub(position);
                }
                reader.notify_seeked(position);
            }
            SummaryReadEvent::ReadRequest(requested) => {
                stats.summary_reads += 1;
                let remaining =
                    usize::try_from(file_size.saturating_sub(position)).unwrap_or(usize::MAX);
                let read_length = requested.min(remaining);
                let start = usize::try_from(position)
                    .map_err(|_| mcap_error("range offset does not fit in usize"))?;
                let bytes = if read_length == 0 {
                    Bytes::new()
                } else if !is_remote {
                    stats.summary_fetches += 1;
                    fetch_range(uri, start, read_length, io_client, io_stats).await?
                } else if let Some(bytes) = read_buffer
                    .as_ref()
                    .and_then(|buffer| buffer.slice(position, read_length))
                {
                    stats.summary_buffer_hits += 1;
                    bytes
                } else {
                    stats.summary_fetches += 1;
                    // The footer seek gives us summary_start, so remaining is
                    // the exact tail size. Fetch it in one piece when it fits
                    // the read-ahead budget; bound memory for larger summaries.
                    let fetch_length = read_length.max(SUMMARY_READ_AHEAD_BYTES).min(remaining);
                    let buffer = BufferedRange {
                        start: position,
                        bytes: fetch_exact_range(uri, start, fetch_length, io_client, io_stats)
                            .await?,
                    };
                    let bytes = buffer.slice(position, read_length).ok_or_else(|| {
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
#[cfg(test)]
pub(crate) fn parse_summary_from_bytes(bytes: &Bytes) -> DaftResult<Option<mcap::Summary>> {
    parse_summary_from_bytes_with_stats(bytes, &mut McapReadStats::default())
}

pub(crate) fn parse_summary_from_bytes_with_stats(
    bytes: &Bytes,
    stats: &mut McapReadStats,
) -> DaftResult<Option<mcap::Summary>> {
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
                stats.summary_seeks += 1;
                position = seek_position(request, position, file_size)?;
                if stats.summary_seeks == 2 {
                    stats.summary_tail_bytes = file_size.checked_sub(position);
                }
                reader.notify_seeked(position);
            }
            SummaryReadEvent::ReadRequest(requested) => {
                stats.summary_reads += 1;
                let start = usize::try_from(position)
                    .map_err(|_| mcap_error("summary position does not fit in usize"))?;
                if start > bytes.len() {
                    return Err(mcap_error("summary offset exceeds file size"));
                }
                let read_length = bytes.len().saturating_sub(start).min(requested);
                if read_length > 0 {
                    stats.summary_buffer_hits += 1;
                }
                reader.insert(requested)[..read_length]
                    .copy_from_slice(&bytes[start..start + read_length]);
                reader.notify_read(read_length);
                position = position.saturating_add(read_length as u64);
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

#[cfg(test)]
mod tests {
    use super::BufferedRange;

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
}
