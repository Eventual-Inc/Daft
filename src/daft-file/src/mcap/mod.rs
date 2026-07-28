pub mod functions;

#[cfg(feature = "python")]
mod python;

use std::io::{Read, Seek, SeekFrom};

use common_error::{DaftError, DaftResult};
use mcap::{
    MAGIC, Summary,
    records::Record,
    sans_io::{SummaryReadEvent, SummaryReader, SummaryReaderOptions},
};
#[cfg(feature = "python")]
pub(crate) use python::register_modules as register_python_modules;

use crate::DaftFile;

const MAX_METADATA_RECORD_SIZE: usize = 256 * 1024 * 1024;
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

fn read_record_at(
    file: &mut DaftFile,
    file_size: u64,
    position: u64,
    expected_length: Option<u64>,
) -> DaftResult<Record<'static>> {
    let remaining = file_size.checked_sub(position).ok_or_else(|| {
        mcap_error(format!(
            "record position {position} exceeds file size {file_size}"
        ))
    })?;
    if remaining < 9 {
        return Err(mcap_error(format!(
            "record at byte {position} does not contain a complete record header"
        )));
    }

    file.seek(SeekFrom::Start(position)).map_err(mcap_error)?;

    let mut record_header = [0_u8; 9];
    file.read_exact(&mut record_header).map_err(mcap_error)?;
    let opcode = record_header[0];
    let body_size = u64::from_le_bytes(
        record_header[1..]
            .try_into()
            .expect("record header always contains an eight-byte size"),
    );
    let record_size = body_size
        .checked_add(9)
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
    Ok(mcap::parse_record(opcode, &body)
        .map_err(mcap_error)?
        .into_owned())
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

/// Read the optional MCAP summary using bounded seeks and reads.
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
