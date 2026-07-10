use std::io::{Read, Seek, SeekFrom};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use common_error::{DaftError, DaftResult};
use mcap::{
    MAGIC, Summary,
    records::Record,
    sans_io::{SummaryReadEvent, SummaryReader, SummaryReaderOptions},
};
use serde::{Serialize, Serializer, ser::SerializeStruct};

use crate::DaftFile;

const MAX_METADATA_RECORD_SIZE: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, Serialize)]
pub struct HeaderMetadata {
    pub profile: String,
    pub library: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StatisticsMetadata {
    pub message_count: u64,
    pub schema_count: u16,
    pub channel_count: u32,
    pub attachment_count: u32,
    pub metadata_count: u32,
    pub chunk_count: u32,
    pub message_start_time: u64,
    pub message_end_time: u64,
}

#[derive(Clone, Debug)]
pub struct SchemaMetadata {
    pub id: u16,
    pub name: String,
    pub encoding: String,
    pub data_size: usize,
    pub data: Option<Vec<u8>>,
}

impl Serialize for SchemaMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SchemaMetadata", 5)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("encoding", &self.encoding)?;
        state.serialize_field("data_size", &self.data_size)?;
        let data_base64 = self.data.as_ref().map(|data| BASE64.encode(data));
        state.serialize_field("data_base64", &data_base64)?;
        state.end()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ChannelMetadata {
    pub id: u16,
    pub topic: String,
    pub message_encoding: String,
    pub schema_id: Option<u16>,
    pub schema_name: Option<String>,
    pub message_count: Option<u64>,
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChunkMetadata {
    pub message_start_time: u64,
    pub message_end_time: u64,
    pub position: u64,
    pub size: u64,
    pub compression: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub message_index_length: u64,
    pub message_index_offsets: Vec<MessageIndexMetadata>,
    pub channel_ids: Vec<u16>,
    pub topics: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MessageIndexMetadata {
    pub channel_id: u16,
    pub position: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AttachmentIndexMetadata {
    pub position: u64,
    pub size: u64,
    pub log_time: u64,
    pub create_time: u64,
    pub data_size: u64,
    pub name: String,
    pub media_type: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetadataRecord {
    pub name: String,
    pub metadata: std::collections::BTreeMap<String, String>,
    pub position: u64,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct McapMetadata {
    pub file_size: u64,
    pub has_summary: bool,
    /// Compatibility shorthand for `has_chunk_indexes`.
    pub indexed: bool,
    pub has_chunk_indexes: bool,
    pub has_message_indexes: bool,
    pub header: HeaderMetadata,
    pub statistics: Option<StatisticsMetadata>,
    pub schemas: Vec<SchemaMetadata>,
    pub channels: Vec<ChannelMetadata>,
    pub chunks: Vec<ChunkMetadata>,
    pub attachments: Vec<AttachmentIndexMetadata>,
    pub metadata: Vec<MetadataRecord>,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadMetadataOptions {
    pub include_schema_data: bool,
    pub include_metadata_records: bool,
    pub include_chunk_indexes: bool,
}

impl Default for ReadMetadataOptions {
    fn default() -> Self {
        Self {
            include_schema_data: false,
            include_metadata_records: false,
            include_chunk_indexes: true,
        }
    }
}

impl ReadMetadataOptions {
    pub const fn full() -> Self {
        Self {
            include_schema_data: true,
            include_metadata_records: true,
            include_chunk_indexes: true,
        }
    }
}

fn mcap_error(error: impl std::fmt::Display) -> DaftError {
    DaftError::ComputeError(format!("Failed to read MCAP metadata: {error}"))
}

pub fn has_magic(file: &mut DaftFile) -> DaftResult<bool> {
    file.seek(SeekFrom::Start(0)).map_err(mcap_error)?;
    let mut magic = [0_u8; 8];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(magic == MAGIC),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(mcap_error(error)),
    }
}

fn read_record_at(file: &mut DaftFile, position: u64) -> DaftResult<Record<'static>> {
    file.seek(SeekFrom::Start(position)).map_err(mcap_error)?;

    let mut record_header = [0_u8; 9];
    file.read_exact(&mut record_header).map_err(mcap_error)?;
    let opcode = record_header[0];
    let size = u64::from_le_bytes(
        record_header[1..]
            .try_into()
            .expect("record header always contains an eight-byte size"),
    );
    let size: usize = size
        .try_into()
        .map_err(|_| mcap_error(format!("record at byte {position} is too large")))?;
    if size > MAX_METADATA_RECORD_SIZE {
        return Err(mcap_error(format!(
            "record at byte {position} exceeds the {MAX_METADATA_RECORD_SIZE}-byte metadata limit"
        )));
    }

    let mut body = vec![0_u8; size];
    file.read_exact(&mut body).map_err(mcap_error)?;
    Ok(mcap::parse_record(opcode, &body)
        .map_err(mcap_error)?
        .into_owned())
}

fn read_header(file: &mut DaftFile) -> DaftResult<HeaderMetadata> {
    if !has_magic(file)? {
        return Err(mcap_error("bad leading magic"));
    }

    match read_record_at(file, MAGIC.len() as u64)? {
        Record::Header(header) => Ok(HeaderMetadata {
            profile: header.profile,
            library: header.library,
        }),
        other => Err(mcap_error(format!(
            "expected a header record, found opcode {:#04x}",
            other.opcode()
        ))),
    }
}

fn read_summary_with_size(file: &mut DaftFile, file_size: u64) -> DaftResult<Option<Summary>> {
    let mut reader = SummaryReader::new_with_options(
        SummaryReaderOptions::default()
            .with_file_size(file_size)
            .with_record_length_limit(MAX_METADATA_RECORD_SIZE),
    );

    while let Some(event) = reader.next_event() {
        match event.map_err(mcap_error)? {
            SummaryReadEvent::ReadRequest(size) => {
                let bytes_read = file.read(reader.insert(size)).map_err(mcap_error)?;
                reader.notify_read(bytes_read);
            }
            SummaryReadEvent::SeekRequest(position) => {
                let position = file.seek(position).map_err(mcap_error)?;
                reader.notify_seeked(position);
            }
        }
    }

    Ok(reader.finish())
}

/// Read the footer summary using bounded seeks and reads.
pub fn read_summary(file: &mut DaftFile) -> DaftResult<Option<Summary>> {
    let file_size: u64 = file
        .size()?
        .try_into()
        .map_err(|_| mcap_error("file size does not fit in u64"))?;
    read_summary_with_size(file, file_size)
}

pub fn read_metadata(file: &mut DaftFile) -> DaftResult<McapMetadata> {
    read_metadata_with_options(file, ReadMetadataOptions::default())
}

pub fn read_metadata_with_options(
    file: &mut DaftFile,
    options: ReadMetadataOptions,
) -> DaftResult<McapMetadata> {
    let file_size: u64 = file
        .size()?
        .try_into()
        .map_err(|_| mcap_error("file size does not fit in u64"))?;
    let header = read_header(file)?;
    let Some(summary) = read_summary_with_size(file, file_size)? else {
        return Ok(McapMetadata {
            file_size,
            has_summary: false,
            indexed: false,
            has_chunk_indexes: false,
            has_message_indexes: false,
            header,
            statistics: None,
            schemas: vec![],
            channels: vec![],
            chunks: vec![],
            attachments: vec![],
            metadata: vec![],
        });
    };

    let statistics = summary.stats.as_ref().map(|stats| StatisticsMetadata {
        message_count: stats.message_count,
        schema_count: stats.schema_count,
        channel_count: stats.channel_count,
        attachment_count: stats.attachment_count,
        metadata_count: stats.metadata_count,
        chunk_count: stats.chunk_count,
        message_start_time: stats.message_start_time,
        message_end_time: stats.message_end_time,
    });

    let mut schemas = summary
        .schemas
        .values()
        .map(|schema| SchemaMetadata {
            id: schema.id,
            name: schema.name.clone(),
            encoding: schema.encoding.clone(),
            data_size: schema.data.len(),
            data: options.include_schema_data.then(|| schema.data.to_vec()),
        })
        .collect::<Vec<_>>();
    schemas.sort_by_key(|schema| schema.id);

    let mut channels = summary
        .channels
        .values()
        .map(|channel| ChannelMetadata {
            id: channel.id,
            topic: channel.topic.clone(),
            message_encoding: channel.message_encoding.clone(),
            schema_id: channel.schema.as_ref().map(|schema| schema.id),
            schema_name: channel.schema.as_ref().map(|schema| schema.name.clone()),
            message_count: summary
                .stats
                .as_ref()
                .and_then(|stats| stats.channel_message_counts.get(&channel.id).copied()),
            metadata: channel.metadata.clone(),
        })
        .collect::<Vec<_>>();
    channels.sort_by_key(|channel| channel.id);

    let has_chunk_indexes = !summary.chunk_indexes.is_empty();
    let has_message_indexes = summary
        .chunk_indexes
        .iter()
        .any(|chunk| !chunk.message_index_offsets.is_empty());

    let chunks = if options.include_chunk_indexes {
        summary
            .chunk_indexes
            .iter()
            .map(|chunk| {
                let channel_ids = chunk
                    .message_index_offsets
                    .keys()
                    .copied()
                    .collect::<Vec<_>>();
                let topics = channel_ids
                    .iter()
                    .filter_map(|channel_id| summary.channels.get(channel_id))
                    .map(|channel| channel.topic.clone())
                    .collect::<Vec<_>>();
                ChunkMetadata {
                    message_start_time: chunk.message_start_time,
                    message_end_time: chunk.message_end_time,
                    position: chunk.chunk_start_offset,
                    size: chunk.chunk_length,
                    compression: chunk.compression.clone(),
                    compressed_size: chunk.compressed_size,
                    uncompressed_size: chunk.uncompressed_size,
                    message_index_length: chunk.message_index_length,
                    message_index_offsets: chunk
                        .message_index_offsets
                        .iter()
                        .map(|(channel_id, position)| MessageIndexMetadata {
                            channel_id: *channel_id,
                            position: *position,
                        })
                        .collect(),
                    channel_ids,
                    topics,
                }
            })
            .collect()
    } else {
        vec![]
    };

    let attachments = summary
        .attachment_indexes
        .iter()
        .map(|attachment| AttachmentIndexMetadata {
            position: attachment.offset,
            size: attachment.length,
            log_time: attachment.log_time,
            create_time: attachment.create_time,
            data_size: attachment.data_size,
            name: attachment.name.clone(),
            media_type: attachment.media_type.clone(),
        })
        .collect();

    let mut metadata = Vec::with_capacity(summary.metadata_indexes.len());
    if options.include_metadata_records {
        for index in &summary.metadata_indexes {
            match read_record_at(file, index.offset)? {
                Record::Metadata(record) => metadata.push(MetadataRecord {
                    name: record.name,
                    metadata: record.metadata,
                    position: index.offset,
                    size: index.length,
                }),
                other => {
                    return Err(mcap_error(format!(
                        "metadata index at byte {} points to opcode {:#04x}",
                        index.offset,
                        other.opcode()
                    )));
                }
            }
        }
    }

    Ok(McapMetadata {
        file_size,
        has_summary: true,
        indexed: has_chunk_indexes,
        has_chunk_indexes,
        has_message_indexes,
        header,
        statistics,
        schemas,
        channels,
        chunks,
        attachments,
        metadata,
    })
}

pub(crate) fn metadata_json(
    file: &mut DaftFile,
    options: ReadMetadataOptions,
) -> DaftResult<String> {
    serde_json::to_string(&read_metadata_with_options(file, options)?).map_err(mcap_error)
}
