use std::{
    collections::{BTreeMap, BTreeSet},
    io::SeekFrom,
    sync::Arc,
};

use bytes::Bytes;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::{BinaryArray, IntoSeries, Series, UInt32Array, UInt64Array, Utf8Array};
use daft_io::{CountingReader, GetRange, GetResult, IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::StreamExt;
use mcap::{
    records::{MessageHeader, Record},
    sans_io::{
        IndexedReadEvent, IndexedReader, IndexedReaderOptions, SummaryReadEvent, SummaryReader,
        SummaryReaderOptions, indexed_reader::ReadOrder,
    },
};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

#[cfg(feature = "python")]
pub mod python;

#[cfg(test)]
mod tests;

const MAX_RECORD_LENGTH: usize = 1024 * 1024 * 1024;

fn mcap_error(error: impl std::fmt::Display) -> DaftError {
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

async fn fetch_exact_range(
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

async fn read_summary(
    uri: &str,
    file_size: usize,
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

    while let Some(event) = reader.next_event() {
        match event.map_err(mcap_error)? {
            SummaryReadEvent::SeekRequest(request) => {
                position = seek_position(request, position, file_size_u64)?;
                reader.notify_seeked(position);
            }
            SummaryReadEvent::ReadRequest(requested) => {
                let remaining = file_size_u64.saturating_sub(position);
                let available = requested.min(usize::try_from(remaining).unwrap_or(usize::MAX));
                let bytes = fetch_range(uri, position, available, io_client, io_stats).await?;
                let read = bytes.len().min(requested);
                reader.insert(requested)[..read].copy_from_slice(&bytes[..read]);
                reader.notify_read(read);
                position = position.saturating_add(read as u64);
            }
        }
    }

    Ok(reader.finish())
}

type AsyncInput = Box<dyn AsyncRead + Send + Unpin>;
type LinearReader = mcap::tokio::LinearReader<AsyncInput>;

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
    let options = mcap::tokio::LinearReaderOptions::default()
        .with_prevalidate_chunk_crcs(true)
        .with_record_length_limit(MAX_RECORD_LENGTH);
    Ok(LinearReader::new_with_options(input, &options))
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
        let file_size = io_client
            .single_url_get_size(uri.clone(), Some(io_stats.clone()))
            .await?;
        if file_size < mcap::MAGIC.len() {
            return Err(mcap_error("file is shorter than MCAP magic"));
        }
        let magic = fetch_exact_range(&uri, 0, mcap::MAGIC.len(), &io_client, &io_stats).await?;
        if magic.as_ref() != mcap::MAGIC {
            return Err(mcap_error("bad leading magic"));
        }

        let topics = options
            .topics
            .map(|topics| topics.into_iter().collect::<BTreeSet<_>>());
        let empty = topics.as_ref().is_some_and(BTreeSet::is_empty)
            || options
                .start_time
                .zip(options.end_time)
                .is_some_and(|(start, end)| start >= end);

        let mut channels = BTreeMap::new();
        let (mode, indexed) = if empty {
            (ReaderMode::Empty, false)
        } else {
            let summary = read_summary(&uri, file_size, &io_client, &io_stats).await?;
            if let Some(summary) = &summary {
                channels.extend(
                    summary
                        .channels
                        .iter()
                        .map(|(id, channel)| (*id, channel.topic.clone())),
                );
            }

            if let Some(summary) = summary
                .filter(|summary| !summary.chunk_indexes.is_empty() && !summary.channels.is_empty())
            {
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
                (
                    ReaderMode::Indexed(
                        IndexedReader::new_with_options(&summary, reader_options)
                            .map_err(mcap_error)?,
                    ),
                    true,
                )
            } else {
                (
                    ReaderMode::Linear {
                        reader: open_linear_reader(&uri, &io_client, &io_stats).await?,
                        record_buffer: Vec::new(),
                    },
                    false,
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
                        let bytes = fetch_exact_range(
                            &self.uri,
                            offset,
                            length,
                            &self.io_client,
                            &self.io_stats,
                        )
                        .await?;
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
