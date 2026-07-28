pub mod functions;

#[cfg(feature = "python")]
mod python;

use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
};

use common_error::{DaftError, DaftResult};
use mcap::{
    MAGIC, Schema,
    records::{self, Record},
};
#[cfg(feature = "python")]
pub(crate) use python::register_modules as register_python_modules;

use crate::DaftFile;

const MAX_METADATA_RECORD_SIZE: usize = 256 * 1024 * 1024;
const RECORD_HEADER_SIZE: u64 = 9;
const FOOTER_RECORD_SIZE: u64 = RECORD_HEADER_SIZE + 20;
pub const BUFFER_SIZE_MCAP_METADATA: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
pub struct HeaderMetadata {
    pub profile: String,
    pub library: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
pub struct SchemaMetadata {
    pub id: u16,
    pub name: String,
    pub encoding: String,
    pub data_size: u64,
    pub data: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
pub struct ChannelMetadata {
    pub id: u16,
    pub topic: String,
    pub message_encoding: String,
    pub schema_id: Option<u16>,
    pub schema_name: Option<String>,
    pub message_count: Option<u64>,
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
pub struct MessageIndexMetadata {
    pub channel_id: u16,
    pub position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
pub struct AttachmentIndexMetadata {
    pub position: u64,
    pub size: u64,
    pub log_time: u64,
    pub create_time: u64,
    pub data_size: u64,
    pub name: String,
    pub media_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
pub struct MetadataRecord {
    pub name: String,
    pub metadata: std::collections::BTreeMap<String, String>,
    pub position: u64,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
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

fn read_record_with_size_at(
    file: &mut DaftFile,
    file_size: u64,
    position: u64,
    expected_length: Option<u64>,
) -> DaftResult<(Record<'static>, u64)> {
    let remaining = file_size.checked_sub(position).ok_or_else(|| {
        mcap_error(format!(
            "record position {position} exceeds file size {file_size}"
        ))
    })?;
    if remaining < RECORD_HEADER_SIZE {
        return Err(mcap_error(format!(
            "record at byte {position} does not contain a complete record header"
        )));
    }

    file.seek(SeekFrom::Start(position)).map_err(mcap_error)?;

    let mut record_header = [0_u8; RECORD_HEADER_SIZE as usize];
    file.read_exact(&mut record_header).map_err(mcap_error)?;
    let opcode = record_header[0];
    let body_size = u64::from_le_bytes(
        record_header[1..]
            .try_into()
            .expect("record header always contains an eight-byte size"),
    );
    let record_size = body_size
        .checked_add(RECORD_HEADER_SIZE)
        .ok_or_else(|| mcap_error(format!("record at byte {position} is too large")))?;
    if record_size > remaining {
        return Err(mcap_error(format!(
            "record at byte {position} extends past the end of the {file_size}-byte file"
        )));
    }
    if let Some(expected_length) = expected_length
        && record_size != expected_length
    {
        return Err(mcap_error(format!(
            "record at byte {position} has length {record_size}, but its index declares {expected_length}"
        )));
    }

    let body_size: usize = body_size
        .try_into()
        .map_err(|_| mcap_error(format!("record at byte {position} is too large")))?;
    if body_size > MAX_METADATA_RECORD_SIZE {
        return Err(mcap_error(format!(
            "record at byte {position} exceeds the {MAX_METADATA_RECORD_SIZE}-byte metadata limit"
        )));
    }

    let mut body = vec![0_u8; body_size];
    file.read_exact(&mut body).map_err(mcap_error)?;
    Ok((
        mcap::parse_record(opcode, &body)
            .map_err(mcap_error)?
            .into_owned(),
        record_size,
    ))
}

fn read_record_at(
    file: &mut DaftFile,
    file_size: u64,
    position: u64,
    expected_length: Option<u64>,
) -> DaftResult<Record<'static>> {
    read_record_with_size_at(file, file_size, position, expected_length).map(|(record, _)| record)
}

fn read_header(file: &mut DaftFile, file_size: u64) -> DaftResult<HeaderMetadata> {
    if !has_magic(file)? {
        return Err(mcap_error("bad leading magic"));
    }

    match read_record_at(file, file_size, MAGIC.len() as u64, None)? {
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

struct ParsedSummary {
    stats: Option<records::Statistics>,
    channels: HashMap<u16, records::Channel>,
    schemas: HashMap<u16, Schema<'static>>,
    chunk_indexes: Vec<records::ChunkIndex>,
    attachment_indexes: Vec<records::AttachmentIndex>,
    metadata_indexes: Vec<records::MetadataIndex>,
}

fn read_summary_with_size(
    file: &mut DaftFile,
    file_size: u64,
) -> DaftResult<Option<ParsedSummary>> {
    let footer_position = file_size
        .checked_sub(FOOTER_RECORD_SIZE + MAGIC.len() as u64)
        .ok_or_else(|| mcap_error("file is too small to contain an MCAP footer"))?;

    file.seek(SeekFrom::Start(footer_position + FOOTER_RECORD_SIZE))
        .map_err(mcap_error)?;
    let mut trailing_magic = [0_u8; MAGIC.len()];
    file.read_exact(&mut trailing_magic).map_err(mcap_error)?;
    if trailing_magic != MAGIC {
        return Err(mcap_error("bad trailing magic"));
    }

    let footer = match read_record_at(file, file_size, footer_position, Some(FOOTER_RECORD_SIZE))? {
        Record::Footer(footer) => footer,
        other => {
            return Err(mcap_error(format!(
                "expected a footer record, found opcode {:#04x}",
                other.opcode()
            )));
        }
    };
    if footer.summary_start == 0 {
        return Ok(None);
    }

    let summary_end = if footer.summary_offset_start == 0 {
        footer_position
    } else {
        footer.summary_offset_start
    };
    if footer.summary_start > summary_end || summary_end > footer_position {
        return Err(mcap_error(format!(
            "invalid summary range {}..{summary_end} for footer at byte {footer_position}",
            footer.summary_start
        )));
    }

    let mut summary = ParsedSummary {
        stats: None,
        channels: HashMap::new(),
        schemas: HashMap::new(),
        chunk_indexes: Vec::new(),
        attachment_indexes: Vec::new(),
        metadata_indexes: Vec::new(),
    };
    let mut position = footer.summary_start;
    while position < summary_end {
        let (record, record_size) = read_record_with_size_at(file, file_size, position, None)?;
        let next_position = position
            .checked_add(record_size)
            .ok_or_else(|| mcap_error(format!("record at byte {position} is too large")))?;
        if next_position > summary_end {
            return Err(mcap_error(format!(
                "summary record at byte {position} extends past the summary boundary at byte {summary_end}"
            )));
        }

        match record {
            Record::AttachmentIndex(index) => summary.attachment_indexes.push(index),
            Record::MetadataIndex(index) => summary.metadata_indexes.push(index),
            Record::Statistics(statistics) => summary.stats = Some(statistics),
            Record::Schema { header, data } => {
                if header.id == 0 {
                    return Err(mcap_error("schema records cannot use ID 0"));
                }
                let schema = Schema {
                    id: header.id,
                    name: header.name,
                    encoding: header.encoding,
                    data,
                };
                if let Some(existing) = summary.schemas.get(&schema.id) {
                    if existing != &schema {
                        return Err(mcap_error(format!(
                            "conflicting schema records use ID {}",
                            schema.id
                        )));
                    }
                } else {
                    summary.schemas.insert(schema.id, schema);
                }
            }
            Record::Channel(channel) => {
                if let Some(existing) = summary.channels.get(&channel.id) {
                    if existing != &channel {
                        return Err(mcap_error(format!(
                            "conflicting channel records use ID {}",
                            channel.id
                        )));
                    }
                } else {
                    summary.channels.insert(channel.id, channel);
                }
            }
            Record::ChunkIndex(index) => summary.chunk_indexes.push(index),
            _ => {}
        }
        position = next_position;
    }

    Ok(Some(summary))
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

    let header = read_header(file, file_size)?;
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
            data_size: schema.data.len() as u64,
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
            schema_id: (channel.schema_id != 0).then_some(channel.schema_id),
            schema_name: summary
                .schemas
                .get(&channel.schema_id)
                .map(|schema| schema.name.clone()),
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
            match read_record_at(file, file_size, index.offset, Some(index.length))? {
                Record::Metadata(record) => {
                    if record.name != index.name {
                        return Err(mcap_error(format!(
                            "metadata index at byte {} names {:?}, but the record names {:?}",
                            index.offset, index.name, record.name
                        )));
                    }
                    metadata.push(MetadataRecord {
                        name: record.name,
                        metadata: record.metadata,
                        position: index.offset,
                        size: index.length,
                    });
                }
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

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, io::Cursor, sync::Arc};

    use daft_schema::media_type::MediaType;
    use mcap::{Attachment, Channel, Message, Schema, WriteOptions, records};

    use super::*;

    fn sample_mcap(options: WriteOptions, include_message: bool) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = options
                .compression(None)
                .profile("test-profile")
                .library("test-library")
                .create(&mut output)
                .unwrap();

            if include_message {
                let schema = Arc::new(Schema {
                    id: 1,
                    name: "example.Message".to_string(),
                    encoding: "protobuf".to_string(),
                    data: Cow::Borrowed(b"schema-bytes"),
                });
                let channel = Arc::new(Channel {
                    id: 1,
                    schema: Some(schema),
                    topic: "/state".to_string(),
                    message_encoding: "protobuf".to_string(),
                    metadata: [("role".to_string(), "state".to_string())].into(),
                });
                writer
                    .write(&Message {
                        channel,
                        sequence: 7,
                        log_time: 100,
                        publish_time: 101,
                        data: Cow::Borrowed(b"payload"),
                    })
                    .unwrap();
                writer
                    .write_metadata(&records::Metadata {
                        name: "session".to_string(),
                        metadata: [("episode".to_string(), "one".to_string())].into(),
                    })
                    .unwrap();
                writer
                    .attach(&Attachment {
                        log_time: 100,
                        create_time: 90,
                        name: "calibration.json".to_string(),
                        media_type: "application/json".to_string(),
                        data: Cow::Borrowed(b"{}"),
                    })
                    .unwrap();
            }

            writer.finish().unwrap();
        }
        output.into_inner()
    }

    #[test]
    fn reads_header_summary_and_indexed_metadata() {
        let bytes = sample_mcap(WriteOptions::default(), true);
        let mut file = DaftFile::from_bytes(MediaType::Mcap, bytes);

        let metadata = read_metadata_with_options(&mut file, ReadMetadataOptions::full()).unwrap();

        assert_eq!(
            metadata.header,
            HeaderMetadata {
                profile: "test-profile".to_string(),
                library: "test-library".to_string(),
            }
        );
        assert_eq!(metadata.statistics.as_ref().unwrap().message_count, 1);
        assert_eq!(
            metadata.statistics.as_ref().unwrap().message_start_time,
            100
        );
        assert_eq!(
            metadata.schemas[0].data.as_deref(),
            Some(b"schema-bytes".as_slice())
        );
        assert_eq!(metadata.channels[0].topic, "/state");
        assert_eq!(metadata.channels[0].message_count, Some(1));
        assert!(metadata.has_chunk_indexes);
        assert!(metadata.has_message_indexes);
        assert_eq!(metadata.attachments[0].name, "calibration.json");
        assert_eq!(metadata.attachments[0].data_size, 2);
        assert_eq!(metadata.metadata[0].name, "session");
        assert_eq!(metadata.metadata[0].metadata["episode"], "one");
    }

    #[test]
    fn options_only_control_materialized_catalog_fields() {
        let bytes = sample_mcap(WriteOptions::default(), true);
        let mut file = DaftFile::from_bytes(MediaType::Mcap, bytes);

        let metadata = read_metadata_with_options(
            &mut file,
            ReadMetadataOptions {
                include_schema_data: false,
                include_metadata_records: false,
                include_chunk_indexes: false,
            },
        )
        .unwrap();

        assert_eq!(metadata.schemas[0].data, None);
        assert!(metadata.chunks.is_empty());
        assert!(metadata.metadata.is_empty());
        assert!(metadata.has_chunk_indexes);
        assert_eq!(metadata.statistics.unwrap().metadata_count, 1);
    }

    #[test]
    fn preserves_schema_id_when_summary_omits_schemas() {
        let bytes = sample_mcap(
            WriteOptions::default()
                .repeat_channels(true)
                .repeat_schemas(false),
            true,
        );
        let mut file = DaftFile::from_bytes(MediaType::Mcap, bytes);

        let metadata = read_metadata(&mut file).unwrap();

        assert!(metadata.schemas.is_empty());
        assert_eq!(metadata.channels[0].schema_id, Some(1));
        assert_eq!(metadata.channels[0].schema_name, None);
    }

    #[test]
    fn reports_absent_summary_without_scanning_data() {
        let bytes = sample_mcap(
            WriteOptions::default()
                .emit_summary_records(false)
                .emit_summary_offsets(false),
            true,
        );
        let mut file = DaftFile::from_bytes(MediaType::Mcap, bytes);

        let metadata = read_metadata(&mut file).unwrap();

        assert!(!metadata.has_summary);
        assert!(metadata.statistics.is_none());
        assert!(metadata.channels.is_empty());
        assert!(metadata.chunks.is_empty());
    }

    #[test]
    fn rejects_records_that_extend_past_end_of_file_before_allocating() {
        let mut bytes = vec![0_u8; 17];
        bytes[8] = records::op::METADATA;
        bytes[9..17].copy_from_slice(&100_u64.to_le_bytes());
        let mut file = DaftFile::from_bytes(MediaType::Mcap, bytes);

        let error = read_record_at(&mut file, 17, 8, None).unwrap_err();

        assert!(error.to_string().contains("extends past the end"));
    }

    #[test]
    fn rejects_records_whose_index_length_disagrees() {
        let mut bytes = vec![0_u8; 17];
        bytes[8] = records::op::METADATA;
        let mut file = DaftFile::from_bytes(MediaType::Mcap, bytes);

        let error = read_record_at(&mut file, 17, 8, Some(10)).unwrap_err();

        assert!(error.to_string().contains("its index declares 10"));
    }
}
