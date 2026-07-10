use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Seek, SeekFrom},
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::{
    BinaryArray, IntoSeries, Schema, Series, UInt32Array, UInt64Array, Utf8Array,
};
use daft_file::{DaftFile, python::PyFileReference};
use daft_recordbatch::{RecordBatch, python::PyRecordBatch};
use mcap::{
    records::{MessageHeader, Record},
    sans_io::{
        IndexedReadEvent, IndexedReader, IndexedReaderOptions, LinearReadEvent, LinearReader,
        LinearReaderOptions, indexed_reader::ReadOrder,
    },
};
use pyo3::prelude::*;

const BUFFER_SIZE_MCAP: usize = 1024 * 1024;

fn mcap_error(error: impl std::fmt::Display) -> DaftError {
    DaftError::ComputeError(format!("Failed to read MCAP messages: {error}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum McapColumn {
    SourcePath,
    Topic,
    LogTime,
    PublishTime,
    Sequence,
    Data,
}

impl TryFrom<&str> for McapColumn {
    type Error = DaftError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "source_path" => Ok(Self::SourcePath),
            "topic" => Ok(Self::Topic),
            "log_time" => Ok(Self::LogTime),
            "publish_time" => Ok(Self::PublishTime),
            "sequence" => Ok(Self::Sequence),
            "data" => Ok(Self::Data),
            other => Err(DaftError::ValueError(format!(
                "Unknown MCAP projection column: {other}"
            ))),
        }
    }
}

struct OwnedMessage {
    topic: String,
    header: MessageHeader,
    data: Option<Vec<u8>>,
}

struct McapBatchBuilder {
    columns: Vec<McapColumn>,
    topics: Vec<String>,
    log_times: Vec<u64>,
    publish_times: Vec<u64>,
    sequences: Vec<u32>,
    data: Vec<Vec<u8>>,
    len: usize,
}

impl McapBatchBuilder {
    fn new(columns: Vec<McapColumn>, capacity: usize) -> Self {
        Self {
            columns,
            topics: Vec::with_capacity(capacity),
            log_times: Vec::with_capacity(capacity),
            publish_times: Vec::with_capacity(capacity),
            sequences: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
            len: 0,
        }
    }

    fn push(&mut self, message: OwnedMessage) {
        let OwnedMessage {
            topic,
            header,
            data,
        } = message;
        if self.columns.contains(&McapColumn::Topic) {
            self.topics.push(topic);
        }
        if self.columns.contains(&McapColumn::LogTime) {
            self.log_times.push(header.log_time);
        }
        if self.columns.contains(&McapColumn::PublishTime) {
            self.publish_times.push(header.publish_time);
        }
        if self.columns.contains(&McapColumn::Sequence) {
            self.sequences.push(header.sequence);
        }
        if self.columns.contains(&McapColumn::Data) {
            self.data.push(data.expect("data projection checked"));
        }
        self.len += 1;
    }

    fn finish(self, source_path: &str) -> DaftResult<RecordBatch> {
        let mut series = Vec::<Series>::with_capacity(self.columns.len());
        let mut topics = Some(self.topics);
        let mut log_times = Some(self.log_times);
        let mut publish_times = Some(self.publish_times);
        let mut sequences = Some(self.sequences);
        let mut data = Some(self.data);

        for column in self.columns {
            series.push(match column {
                McapColumn::SourcePath => Utf8Array::from_values(
                    "source_path",
                    std::iter::repeat_n(source_path, self.len),
                )
                .into_series(),
                McapColumn::Topic => Utf8Array::from_values(
                    "topic",
                    topics.take().expect("topic column emitted once"),
                )
                .into_series(),
                McapColumn::LogTime => UInt64Array::from_vec(
                    "log_time",
                    log_times.take().expect("log_time column emitted once"),
                )
                .into_series(),
                McapColumn::PublishTime => UInt64Array::from_vec(
                    "publish_time",
                    publish_times
                        .take()
                        .expect("publish_time column emitted once"),
                )
                .into_series(),
                McapColumn::Sequence => UInt32Array::from_vec(
                    "sequence",
                    sequences.take().expect("sequence column emitted once"),
                )
                .into_series(),
                McapColumn::Data => {
                    BinaryArray::from_values("data", data.take().expect("data column emitted once"))
                        .into_series()
                }
            });
        }

        if series.is_empty() {
            Ok(RecordBatch::new_unchecked(
                Schema::empty(),
                vec![],
                self.len,
            ))
        } else {
            RecordBatch::from_nonempty_columns(series)
        }
    }
}

enum McapReaderMode {
    Indexed(IndexedReader),
    Linear(LinearReader),
    Empty,
}

struct NativeMcapReader {
    file: DaftFile,
    mode: McapReaderMode,
    channels: BTreeMap<u16, String>,
    topics: Option<BTreeSet<String>>,
    start_time: Option<u64>,
    end_time: Option<u64>,
    columns: Vec<McapColumn>,
    include_data: bool,
    source_path: String,
    batch_size: usize,
    limit: Option<usize>,
    emitted: usize,
    finished: bool,
    indexed: bool,
    chunk_buffer: Vec<u8>,
}

impl NativeMcapReader {
    #[allow(clippy::too_many_arguments)]
    fn new(
        file_ref: daft_core::file::FileReference,
        columns: Vec<String>,
        batch_size: usize,
        start_time: Option<u64>,
        end_time: Option<u64>,
        topics: Option<Vec<String>>,
        limit: Option<usize>,
        reverse: bool,
    ) -> DaftResult<Self> {
        let source_path = file_ref.url.clone();
        let columns = columns
            .iter()
            .map(|column| McapColumn::try_from(column.as_str()))
            .collect::<DaftResult<Vec<_>>>()?;
        let include_data = columns.contains(&McapColumn::Data);
        let topics = topics.map(|topics| topics.into_iter().collect::<BTreeSet<_>>());

        let mut file = DaftFile::load_blocking(file_ref, false, Some(BUFFER_SIZE_MCAP))?;
        if !daft_file::mcap::has_magic(&mut file)? {
            return Err(mcap_error("bad leading magic"));
        }
        let summary = daft_file::mcap::read_summary(&mut file)?;
        let mut channels = BTreeMap::new();
        if let Some(summary) = &summary {
            channels.extend(
                summary
                    .channels
                    .iter()
                    .map(|(id, channel)| (*id, channel.topic.clone())),
            );
        }

        let requested_topics_are_empty = topics.as_ref().is_some_and(BTreeSet::is_empty);
        let summary_channels_are_authoritative = summary
            .as_ref()
            .is_some_and(|summary| !summary.chunk_indexes.is_empty());
        let requested_topics_are_missing = topics.as_ref().is_some_and(|requested| {
            summary_channels_are_authoritative
                && !channels.values().any(|topic| requested.contains(topic))
        });

        let empty_time_range = start_time
            .zip(end_time)
            .is_some_and(|(start, end)| start >= end);
        let (mode, indexed) =
            if requested_topics_are_empty || requested_topics_are_missing || empty_time_range {
                (McapReaderMode::Empty, false)
            } else if let Some(summary) = summary
                .filter(|summary| !summary.chunk_indexes.is_empty() && !summary.channels.is_empty())
            {
                let order = if reverse {
                    ReadOrder::ReverseLogTime
                } else {
                    ReadOrder::LogTime
                };
                let mut options = IndexedReaderOptions::new().with_order(order);
                if let Some(start_time) = start_time {
                    options = options.log_time_on_or_after(start_time);
                }
                if let Some(end_time) = end_time {
                    options = options.log_time_before(end_time);
                }
                if let Some(topics) = &topics {
                    options = options.include_topics(topics.iter().cloned());
                }
                (
                    McapReaderMode::Indexed(
                        IndexedReader::new_with_options(&summary, options).map_err(mcap_error)?,
                    ),
                    true,
                )
            } else if reverse {
                // Reverse reads are an indexed-only optimization. Callers can
                // inspect `indexed` and fall back to a forward scan.
                (McapReaderMode::Empty, false)
            } else {
                file.seek(SeekFrom::Start(0)).map_err(mcap_error)?;
                let options = LinearReaderOptions::default().with_prevalidate_chunk_crcs(true);
                (
                    McapReaderMode::Linear(LinearReader::new_with_options(options)),
                    false,
                )
            };

        Ok(Self {
            file,
            mode,
            channels,
            topics,
            start_time,
            end_time,
            columns,
            include_data,
            source_path,
            batch_size: batch_size.max(1),
            limit,
            emitted: 0,
            finished: false,
            indexed,
            chunk_buffer: Vec::new(),
        })
    }

    fn message_matches(
        topics: &Option<BTreeSet<String>>,
        start_time: Option<u64>,
        end_time: Option<u64>,
        topic: &str,
        header: MessageHeader,
    ) -> bool {
        if start_time.is_some_and(|start| header.log_time < start) {
            return false;
        }
        if end_time.is_some_and(|end| header.log_time >= end) {
            return false;
        }
        topics.as_ref().is_none_or(|topics| topics.contains(topic))
    }

    fn next_message(&mut self) -> DaftResult<Option<OwnedMessage>> {
        loop {
            match &mut self.mode {
                McapReaderMode::Empty => return Ok(None),
                McapReaderMode::Indexed(reader) => {
                    let Some(event) = reader.next_event() else {
                        return Ok(None);
                    };
                    match event.map_err(mcap_error)? {
                        IndexedReadEvent::ReadChunkRequest { offset, length } => {
                            self.file
                                .seek(SeekFrom::Start(offset))
                                .map_err(mcap_error)?;
                            self.chunk_buffer.resize(length, 0);
                            self.file
                                .read_exact(&mut self.chunk_buffer)
                                .map_err(mcap_error)?;
                            reader
                                .insert_chunk_record_data(offset, &self.chunk_buffer)
                                .map_err(mcap_error)?;
                        }
                        IndexedReadEvent::Message { header, data } => {
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
                            if Self::message_matches(
                                &self.topics,
                                self.start_time,
                                self.end_time,
                                &topic,
                                header,
                            ) {
                                return Ok(Some(OwnedMessage {
                                    topic,
                                    header,
                                    data: self.include_data.then(|| data.to_vec()),
                                }));
                            }
                        }
                    }
                }
                McapReaderMode::Linear(reader) => {
                    let Some(event) = reader.next_event() else {
                        return Ok(None);
                    };
                    match event.map_err(mcap_error)? {
                        LinearReadEvent::ReadRequest(needed) => {
                            let bytes_read =
                                self.file.read(reader.insert(needed)).map_err(mcap_error)?;
                            reader.notify_read(bytes_read);
                        }
                        LinearReadEvent::Record { opcode, data } => {
                            match mcap::parse_record(opcode, data).map_err(mcap_error)? {
                                Record::Channel(channel) => {
                                    self.channels.insert(channel.id, channel.topic);
                                }
                                Record::Message { header, data } => {
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
                                    if Self::message_matches(
                                        &self.topics,
                                        self.start_time,
                                        self.end_time,
                                        &topic,
                                        header,
                                    ) {
                                        return Ok(Some(OwnedMessage {
                                            topic,
                                            header,
                                            data: self.include_data.then(|| data.into_owned()),
                                        }));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }

    fn next_batch(&mut self) -> DaftResult<Option<RecordBatch>> {
        if self.finished || self.limit.is_some_and(|limit| self.emitted >= limit) {
            self.finished = true;
            return Ok(None);
        }

        let target = self.limit.map_or(self.batch_size, |limit| {
            self.batch_size.min(limit - self.emitted)
        });
        let mut builder = McapBatchBuilder::new(self.columns.clone(), target);
        while builder.len < target {
            let Some(message) = self.next_message()? else {
                self.finished = true;
                break;
            };
            builder.push(message);
        }

        if builder.len == 0 {
            return Ok(None);
        }
        self.emitted += builder.len;
        Ok(Some(builder.finish(&self.source_path)?))
    }
}

#[pyclass(unsendable)]
pub struct PyMcapReader {
    inner: NativeMcapReader,
}

#[pymethods]
impl PyMcapReader {
    #[new]
    #[pyo3(signature = (file, columns, batch_size=1000, start_time=None, end_time=None, topics=None, limit=None, reverse=false))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        file: &PyFileReference,
        columns: Vec<String>,
        batch_size: usize,
        start_time: Option<u64>,
        end_time: Option<u64>,
        topics: Option<Vec<String>>,
        limit: Option<usize>,
        reverse: bool,
    ) -> PyResult<Self> {
        let file_ref = file.file_reference();
        py.detach(move || {
            Ok(Self {
                inner: NativeMcapReader::new(
                    file_ref, columns, batch_size, start_time, end_time, topics, limit, reverse,
                )?,
            })
        })
    }

    fn next_batch(&mut self, py: Python<'_>) -> PyResult<Option<PyRecordBatch>> {
        py.detach(|| Ok(self.inner.next_batch()?.map(Into::into)))
    }

    #[getter]
    fn indexed(&self) -> bool {
        self.inner.indexed
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyMcapReader>()?;
    Ok(())
}
