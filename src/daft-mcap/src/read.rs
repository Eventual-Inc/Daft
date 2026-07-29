//! The MCAP reader: indexed chunk traversal with a linear fallback.
//!
//! [`NativeMcapReader`] decodes one MCAP file into record batches. Indexed
//! files are read through the summary's chunk indexes in log-time order;
//! files without a usable index stream linearly in file order.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bytes::Bytes;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::{BinaryArray, IntoSeries, Series, UInt32Array, UInt64Array, Utf8Array};
use daft_io::{CountingReader, GetResult, IOClient, IOStatsRef, SourceType, parse_url};
use daft_recordbatch::RecordBatch;
use futures::StreamExt;
use mcap::{
    records::{MessageHeader, Record},
    sans_io::{IndexedReadEvent, IndexedReader, IndexedReaderOptions, indexed_reader::ReadOrder},
};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use crate::{
    BufferedRange, MAX_RECORD_LENGTH, McapReadOptions, fetch_exact_range, mcap_error,
    parse_summary_from_bytes, read_summary,
};

// Read small remote files in one request.
const SMALL_REMOTE_FILE_THRESHOLD: usize = 64 * 1024;
// Coalesce nearby indexed reads without unbounded overfetch.
const INDEXED_READ_AHEAD_MULTIPLIER: usize = 8;
const MAX_INDEXED_READ_AHEAD_BYTES: usize = 8 * 1024 * 1024;

type AsyncInput = Box<dyn AsyncRead + Send + Unpin>;
type LinearReader = mcap::tokio::LinearReader<AsyncInput>;

fn linear_reader(input: AsyncInput) -> LinearReader {
    let options = mcap::tokio::LinearReaderOptions::default()
        .with_prevalidate_chunk_crcs(true)
        .with_record_length_limit(MAX_RECORD_LENGTH);
    LinearReader::new_with_options(input, &options)
}

fn open_buffered_linear_reader(bytes: Bytes) -> LinearReader {
    linear_reader(Box::new(std::io::Cursor::new(bytes)))
}

fn indexed_read_length(requested: usize, available: usize) -> usize {
    requested
        .saturating_mul(INDEXED_READ_AHEAD_MULTIPLIER)
        .min(MAX_INDEXED_READ_AHEAD_BYTES)
        .max(requested)
        .min(available)
}

async fn open_linear_reader(
    uri: &str,
    io_client: &Arc<IOClient>,
    io_stats: &IOStatsRef,
) -> DaftResult<LinearReader> {
    let result = io_client
        .single_url_get(uri.to_string(), None, Some(io_stats.clone()))
        .await?;
    let input: AsyncInput = match result {
        GetResult::File(file) => {
            if file.range.is_some() {
                return Err(DaftError::InternalError(
                    "full MCAP read unexpectedly returned a ranged local file".to_string(),
                ));
            }
            let source = tokio::fs::File::open(&file.path).await.map_err(|error| {
                DaftError::External(
                    std::io::Error::new(
                        error.kind(),
                        format!("failed to open {}: {error}", file.path.display()),
                    )
                    .into(),
                )
            })?;
            Box::new(CountingReader::new(source, Some(io_stats.clone())))
        }
        GetResult::Stream(stream, ..) => Box::new(StreamReader::new(
            stream.map(|result| result.map_err(std::io::Error::from)),
        )),
    };
    Ok(linear_reader(input))
}

struct OwnedMessage {
    topic: String,
    header: MessageHeader,
    data: Vec<u8>,
}

struct McapBatchBuilder {
    source_paths: Vec<String>,
    topics: Vec<String>,
    log_times: Vec<u64>,
    publish_times: Vec<u64>,
    sequences: Vec<u32>,
    data: Vec<Vec<u8>>,
}

impl McapBatchBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            source_paths: Vec::with_capacity(capacity),
            topics: Vec::with_capacity(capacity),
            log_times: Vec::with_capacity(capacity),
            publish_times: Vec::with_capacity(capacity),
            sequences: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.topics.len()
    }

    fn push(&mut self, source_path: &str, message: OwnedMessage) {
        self.source_paths.push(source_path.to_string());
        self.topics.push(message.topic);
        self.log_times.push(message.header.log_time);
        self.publish_times.push(message.header.publish_time);
        self.sequences.push(message.header.sequence);
        self.data.push(message.data);
    }

    fn finish(self) -> DaftResult<RecordBatch> {
        let columns: Vec<Series> = vec![
            Utf8Array::from_values("source_path", self.source_paths).into_series(),
            Utf8Array::from_values("topic", self.topics).into_series(),
            UInt64Array::from_vec("log_time", self.log_times).into_series(),
            UInt64Array::from_vec("publish_time", self.publish_times).into_series(),
            UInt32Array::from_vec("sequence", self.sequences).into_series(),
            BinaryArray::from_values("data", self.data).into_series(),
        ];
        RecordBatch::from_nonempty_columns(columns)
    }
}

enum ReaderMode {
    Indexed(IndexedReader),
    Linear {
        reader: LinearReader,
        record_buffer: Vec<u8>,
    },
    Empty,
}

enum IndexedAction {
    ReadChunk {
        offset: u64,
        length: usize,
    },
    Message {
        header: MessageHeader,
        data: Vec<u8>,
    },
    End,
}

enum LinearAction {
    Channel {
        id: u16,
        topic: String,
    },
    Message {
        header: MessageHeader,
        data: Vec<u8>,
    },
    Ignore,
    End,
}

pub struct NativeMcapReader {
    uri: String,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    mode: ReaderMode,
    channels: BTreeMap<u16, String>,
    topics: Option<BTreeSet<String>>,
    start_time: Option<u64>,
    end_time: Option<u64>,
    batch_size: usize,
    finished: bool,
    indexed: bool,
    indexed_remote_file_size: Option<usize>,
    indexed_read_buffer: Option<BufferedRange>,
}

impl NativeMcapReader {
    pub async fn new(
        uri: impl Into<String>,
        io_client: Arc<IOClient>,
        io_stats: IOStatsRef,
        options: McapReadOptions,
    ) -> DaftResult<Self> {
        if options.batch_size == 0 {
            return Err(DaftError::ValueError(
                "MCAP batch_size must be positive".to_string(),
            ));
        }
        let uri = uri.into();
        let topics = options
            .topics
            .map(|topics| topics.into_iter().collect::<BTreeSet<_>>());
        let empty = topics.as_ref().is_some_and(BTreeSet::is_empty)
            || options
                .start_time
                .zip(options.end_time)
                .is_some_and(|(start, end)| start >= end);

        let mut channels = BTreeMap::new();
        let (mode, indexed, indexed_remote_file_size, indexed_read_buffer) = if empty {
            // Provably-empty constraints never touch storage.
            (ReaderMode::Empty, false, None, None)
        } else {
            let is_remote = !matches!(parse_url(&uri)?.0, SourceType::File);
            let file_size = io_client
                .single_url_get_size(uri.clone(), Some(io_stats.clone()))
                .await?;
            if file_size < mcap::MAGIC.len() {
                return Err(mcap_error("file is shorter than MCAP magic"));
            }

            // Small remote files: one request buys the whole file; the summary
            // and any chunk reads are then served from the buffer.
            let buffer = if is_remote && file_size <= SMALL_REMOTE_FILE_THRESHOLD {
                Some(fetch_exact_range(&uri, 0, file_size, &io_client, &io_stats).await?)
            } else {
                None
            };
            let magic = match &buffer {
                Some(bytes) => bytes.slice(0..mcap::MAGIC.len()),
                None => {
                    fetch_exact_range(&uri, 0, mcap::MAGIC.len(), &io_client, &io_stats).await?
                }
            };
            if magic.as_ref() != mcap::MAGIC {
                return Err(mcap_error("bad leading magic"));
            }

            let summary = match &buffer {
                Some(bytes) => parse_summary_from_bytes(bytes)?,
                None => read_summary(&uri, file_size, is_remote, &io_client, &io_stats).await?,
            };
            if let Some(summary) = &summary {
                channels.extend(
                    summary
                        .channels
                        .iter()
                        .map(|(id, channel)| (*id, channel.topic.clone())),
                );
            }

            let usable = summary.filter(|summary| {
                !summary.chunk_indexes.is_empty() && !summary.channels.is_empty()
            });
            if let Some(summary) = usable {
                // Indexed traversal is the primary path: chunk indexes prune
                // I/O and ReadOrder::LogTime yields per-file log-time order.
                let mut reader_options = IndexedReaderOptions::new().with_order(ReadOrder::LogTime);
                if let Some(start_time) = options.start_time {
                    reader_options = reader_options.log_time_on_or_after(start_time);
                }
                if let Some(end_time) = options.end_time {
                    reader_options = reader_options.log_time_before(end_time);
                }
                if let Some(topics) = &topics {
                    reader_options = reader_options.include_topics(topics.iter().cloned());
                }
                reader_options = reader_options.with_record_length_limit(MAX_RECORD_LENGTH);
                let read_buffer = buffer.map(|bytes| BufferedRange { start: 0, bytes });
                let remote_file_size = (is_remote && read_buffer.is_none()).then_some(file_size);
                (
                    ReaderMode::Indexed(
                        IndexedReader::new_with_options(&summary, reader_options)
                            .map_err(mcap_error)?,
                    ),
                    true,
                    remote_file_size,
                    read_buffer,
                )
            } else {
                // Unindexed fallback: stream records in file order.
                let reader = match buffer {
                    Some(bytes) => open_buffered_linear_reader(bytes),
                    None => open_linear_reader(&uri, &io_client, &io_stats).await?,
                };
                (
                    ReaderMode::Linear {
                        reader,
                        record_buffer: Vec::new(),
                    },
                    false,
                    None,
                    None,
                )
            }
        };

        Ok(Self {
            uri,
            io_client,
            io_stats,
            mode,
            channels,
            topics,
            start_time: options.start_time,
            end_time: options.end_time,
            batch_size: options.batch_size,
            finished: false,
            indexed,
            indexed_remote_file_size,
            indexed_read_buffer,
        })
    }

    pub fn indexed(&self) -> bool {
        self.indexed
    }

    fn message_matches(&self, topic: &str, header: MessageHeader) -> bool {
        if self.start_time.is_some_and(|start| header.log_time < start) {
            return false;
        }
        if self.end_time.is_some_and(|end| header.log_time >= end) {
            return false;
        }
        self.topics
            .as_ref()
            .is_none_or(|topics| topics.contains(topic))
    }

    async fn fetch_indexed_chunk(&mut self, offset: u64, length: usize) -> DaftResult<Bytes> {
        if let Some(bytes) = self
            .indexed_read_buffer
            .as_ref()
            .and_then(|buffer| buffer.slice(offset, length))
        {
            return Ok(bytes);
        }

        let Some(file_size) = self.indexed_remote_file_size else {
            return fetch_exact_range(&self.uri, offset, length, &self.io_client, &self.io_stats)
                .await;
        };
        let start = usize::try_from(offset)
            .map_err(|_| mcap_error("range offset does not fit in usize"))?;
        let available = file_size.checked_sub(start).ok_or_else(|| {
            mcap_error(format!(
                "chunk offset {offset} exceeds file size {file_size}"
            ))
        })?;
        if length > available {
            return Err(mcap_error(format!(
                "unexpected EOF reading range at {offset}: requested {length} bytes, only {available} available"
            )));
        }

        let read_length = indexed_read_length(length, available);
        let buffer = BufferedRange {
            start: offset,
            bytes: fetch_exact_range(
                &self.uri,
                offset,
                read_length,
                &self.io_client,
                &self.io_stats,
            )
            .await?,
        };
        let bytes = buffer.slice(offset, length).ok_or_else(|| {
            DaftError::InternalError(
                "indexed MCAP read-ahead did not contain requested chunk".to_string(),
            )
        })?;
        self.indexed_read_buffer = Some(buffer);
        Ok(bytes)
    }

    fn next_indexed_action(&mut self) -> DaftResult<IndexedAction> {
        let ReaderMode::Indexed(reader) = &mut self.mode else {
            return Err(DaftError::InternalError(
                "indexed action requested from non-indexed MCAP reader".to_string(),
            ));
        };
        let Some(event) = reader.next_event() else {
            return Ok(IndexedAction::End);
        };
        match event.map_err(mcap_error)? {
            IndexedReadEvent::ReadChunkRequest { offset, length } => {
                Ok(IndexedAction::ReadChunk { offset, length })
            }
            IndexedReadEvent::Message { header, data } => Ok(IndexedAction::Message {
                header,
                data: data.to_vec(),
            }),
        }
    }

    async fn next_linear_action(&mut self) -> DaftResult<LinearAction> {
        let ReaderMode::Linear {
            reader,
            record_buffer,
        } = &mut self.mode
        else {
            return Err(DaftError::InternalError(
                "linear action requested from non-linear MCAP reader".to_string(),
            ));
        };
        let Some(opcode) = reader.next_record(record_buffer).await else {
            return Ok(LinearAction::End);
        };
        match mcap::parse_record(opcode.map_err(mcap_error)?, record_buffer).map_err(mcap_error)? {
            Record::Channel(channel) => Ok(LinearAction::Channel {
                id: channel.id,
                topic: channel.topic,
            }),
            Record::Message { header, data } => Ok(LinearAction::Message {
                header,
                data: data.into_owned(),
            }),
            _ => Ok(LinearAction::Ignore),
        }
    }

    async fn next_message(&mut self) -> DaftResult<Option<OwnedMessage>> {
        loop {
            if matches!(self.mode, ReaderMode::Empty) {
                return Ok(None);
            }
            if matches!(self.mode, ReaderMode::Indexed(_)) {
                match self.next_indexed_action()? {
                    IndexedAction::ReadChunk { offset, length } => {
                        let bytes = self.fetch_indexed_chunk(offset, length).await?;
                        let ReaderMode::Indexed(reader) = &mut self.mode else {
                            unreachable!("reader mode changed while fetching MCAP chunk")
                        };
                        reader
                            .insert_chunk_record_data(offset, &bytes)
                            .map_err(mcap_error)?;
                    }
                    IndexedAction::Message { header, data } => {
                        let topic = self
                            .channels
                            .get(&header.channel_id)
                            .ok_or_else(|| {
                                mcap_error(format!(
                                    "message references unknown channel {}",
                                    header.channel_id
                                ))
                            })?
                            .clone();
                        if self.message_matches(&topic, header) {
                            return Ok(Some(OwnedMessage {
                                topic,
                                header,
                                data,
                            }));
                        }
                    }
                    IndexedAction::End => return Ok(None),
                }
                continue;
            }

            match self.next_linear_action().await? {
                LinearAction::Channel { id, topic } => {
                    self.channels.insert(id, topic);
                }
                LinearAction::Message { header, data } => {
                    let topic = self
                        .channels
                        .get(&header.channel_id)
                        .ok_or_else(|| {
                            mcap_error(format!(
                                "message references unknown channel {}",
                                header.channel_id
                            ))
                        })?
                        .clone();
                    if self.message_matches(&topic, header) {
                        return Ok(Some(OwnedMessage {
                            topic,
                            header,
                            data,
                        }));
                    }
                }
                LinearAction::Ignore => {}
                LinearAction::End => return Ok(None),
            }
        }
    }

    pub async fn next_batch(&mut self) -> DaftResult<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        let mut builder = McapBatchBuilder::new(self.batch_size);
        while builder.len() < self.batch_size {
            let Some(message) = self.next_message().await? else {
                self.finished = true;
                break;
            };
            builder.push(&self.uri, message);
        }
        if builder.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(builder.finish()?))
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use mcap::Compression;
    use tempfile::NamedTempFile;

    use super::indexed_read_length;
    use crate::{
        McapReadOptions,
        test_utils::{collect_rows, make_reader, write_mcap, write_mcap_out_of_order},
    };

    #[test]
    fn indexed_read_ahead_is_bounded() {
        assert_eq!(
            indexed_read_length(1024 * 1024, usize::MAX),
            8 * 1024 * 1024
        );
        assert_eq!(
            indexed_read_length(16 * 1024 * 1024, usize::MAX),
            16 * 1024 * 1024
        );
        assert_eq!(
            indexed_read_length(1024 * 1024, 3 * 1024 * 1024),
            3 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn unfiltered_unindexed_reader_streams_file_once() -> DaftResult<()> {
        let file = write_mcap(false, None, 20, 32);
        let file_size = file.as_file().metadata().unwrap().len() as usize;
        let (mut reader, io_stats) = make_reader(&file, McapReadOptions::default()).await?;

        assert!(!reader.indexed());
        assert_eq!(collect_rows(&mut reader).await?.len(), 20);
        drop(reader);
        // One magic probe and one summary probe, then a single linear pass.
        let bytes_read = io_stats.load_bytes_read();
        assert!(
            bytes_read >= file_size && bytes_read < 2 * file_size,
            "unindexed unfiltered read fetched {bytes_read} of {file_size} bytes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn unfiltered_indexed_read_returns_log_time_order() -> DaftResult<()> {
        let file = write_mcap_out_of_order(10);
        // Fixture precondition: chunks must be physically out of log-time order.
        let contents = bytes::Bytes::from(std::fs::read(file.path()).unwrap());
        let summary = crate::parse_summary_from_bytes(&contents)?.expect("fixture has a summary");
        assert!(
            summary
                .chunk_indexes
                .windows(2)
                .any(|pair| pair[0].message_start_time > pair[1].message_start_time),
            "fixture chunks are not out of log-time order"
        );

        let (mut reader, _) = make_reader(&file, McapReadOptions::default()).await?;
        assert!(reader.indexed());
        let times = collect_rows(&mut reader)
            .await?
            .iter()
            .map(|(_, time, _)| *time)
            .collect::<Vec<_>>();
        assert_eq!(times.len(), 20);
        assert_eq!(times.first(), Some(&0));
        let mut sorted = times.clone();
        sorted.sort_unstable();
        assert_eq!(times, sorted);
        Ok(())
    }

    #[tokio::test]
    async fn empty_constraints_skip_io() -> DaftResult<()> {
        let file = write_mcap(true, None, 4, 8);
        let options = McapReadOptions {
            topics: Some(vec![]),
            ..Default::default()
        };
        let (mut reader, io_stats) = make_reader(&file, options).await?;
        assert!(reader.next_batch().await?.is_none());
        assert_eq!(io_stats.load_bytes_read(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn indexed_reader_filters_topics_and_half_open_time_range() -> DaftResult<()> {
        let file = write_mcap(true, None, 20, 32);
        let options = McapReadOptions {
            batch_size: 2,
            start_time: Some(4),
            end_time: Some(10),
            topics: Some(vec!["/camera".to_string()]),
        };
        let (mut reader, _) = make_reader(&file, options).await?;
        assert!(reader.indexed());

        let rows = collect_rows(&mut reader).await?;
        assert_eq!(
            rows.iter()
                .map(|(topic, time, _)| (topic.as_str(), *time))
                .collect::<Vec<_>>(),
            vec![("/camera", 4), ("/camera", 6), ("/camera", 8)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn indexed_reader_uses_bounded_ranges() -> DaftResult<()> {
        let file = write_mcap(true, None, 16, 64 * 1024);
        let file_size = file.as_file().metadata().unwrap().len() as usize;
        let options = McapReadOptions {
            batch_size: 1,
            start_time: Some(8),
            end_time: Some(9),
            topics: Some(vec!["/camera".to_string()]),
        };
        let (mut reader, io_stats) = make_reader(&file, options).await?;
        let rows = collect_rows(&mut reader).await?;

        assert_eq!(rows.len(), 1);
        assert!(
            io_stats.load_bytes_read() < file_size / 2,
            "filtered indexed read fetched {} of {file_size} bytes",
            io_stats.load_bytes_read()
        );
        Ok(())
    }

    #[tokio::test]
    async fn unindexed_reader_falls_back_to_linear_stream() -> DaftResult<()> {
        let file = write_mcap(false, None, 12, 16);
        let options = McapReadOptions {
            batch_size: 3,
            start_time: Some(3),
            end_time: Some(8),
            topics: Some(vec!["/imu".to_string()]),
        };
        let (mut reader, _) = make_reader(&file, options).await?;
        assert!(!reader.indexed());

        let rows = collect_rows(&mut reader).await?;
        assert_eq!(
            rows.iter().map(|(_, time, _)| *time).collect::<Vec<_>>(),
            vec![3, 5, 7]
        );
        Ok(())
    }

    #[tokio::test]
    async fn indexed_reader_supports_lz4() -> DaftResult<()> {
        let file = write_mcap(true, Some(Compression::Lz4), 10, 1024);
        let options = McapReadOptions {
            topics: Some(vec!["/camera".to_string(), "/imu".to_string()]),
            ..Default::default()
        };
        let (mut reader, _) = make_reader(&file, options).await?;
        assert!(reader.indexed());
        assert_eq!(collect_rows(&mut reader).await?.len(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn indexed_reader_supports_zstd() -> DaftResult<()> {
        let file = write_mcap(true, Some(Compression::Zstd), 10, 1024);
        let options = McapReadOptions {
            topics: Some(vec!["/camera".to_string(), "/imu".to_string()]),
            ..Default::default()
        };
        let (mut reader, _) = make_reader(&file, options).await?;
        assert!(reader.indexed());
        assert_eq!(collect_rows(&mut reader).await?.len(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_magic_is_rejected_at_open() -> DaftResult<()> {
        let file = NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"not an mcap file").unwrap();
        let result = make_reader(&file, McapReadOptions::default()).await;
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().to_lowercase().contains("magic"))
        );
        Ok(())
    }
}
