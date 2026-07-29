//! Shared fixtures for the in-module test suites.

use std::{borrow::Cow, collections::BTreeMap, fs::File, io::BufWriter, sync::Arc};

use common_error::DaftResult;
use daft_io::{IOConfig, IOStatsContext, get_io_client};
use futures::TryStreamExt;
use mcap::{Channel, Compression, Message, WriteOptions};
use tempfile::NamedTempFile;

use crate::{McapConvertOptions, McapReadOptions, NativeMcapReader, stream_mcap};

pub(crate) fn write_mcap(
    indexed: bool,
    compression: Option<Compression>,
    message_count: usize,
    payload_size: usize,
) -> NamedTempFile {
    let temp = NamedTempFile::new().unwrap();
    let output = File::create(temp.path()).unwrap();
    let mut writer = WriteOptions::new()
        .use_chunks(indexed)
        .chunk_size(Some((payload_size.max(1) + 256) as u64))
        .compression(compression)
        .create(BufWriter::new(output))
        .unwrap();
    let camera = Arc::new(Channel {
        id: 1,
        schema: None,
        topic: "/camera".to_string(),
        message_encoding: String::new(),
        metadata: BTreeMap::new(),
    });
    let imu = Arc::new(Channel {
        id: 2,
        schema: None,
        topic: "/imu".to_string(),
        message_encoding: String::new(),
        metadata: BTreeMap::new(),
    });

    for index in 0..message_count {
        let channel = if index % 2 == 0 {
            camera.clone()
        } else {
            imu.clone()
        };
        let mut data = vec![index as u8; payload_size];
        data.extend_from_slice(&(index as u64).to_le_bytes());
        writer
            .write(&Message {
                channel,
                sequence: index as u32,
                log_time: index as u64,
                publish_time: index as u64 + 100,
                data: Cow::Owned(data),
            })
            .unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    temp
}

/// Writes an indexed MCAP whose chunks are physically out of log-time order:
/// a run of log times starting at 100 precedes a run starting at 0, with a
/// chunk size small enough that the runs never share a chunk.
pub(crate) fn write_mcap_out_of_order(messages_per_run: usize) -> NamedTempFile {
    let temp = NamedTempFile::new().unwrap();
    let output = File::create(temp.path()).unwrap();
    let mut writer = WriteOptions::new()
        .use_chunks(true)
        .chunk_size(Some(64))
        .create(BufWriter::new(output))
        .unwrap();
    let channel = Arc::new(Channel {
        id: 1,
        schema: None,
        topic: "/camera".to_string(),
        message_encoding: String::new(),
        metadata: BTreeMap::new(),
    });

    for run_start in [100_u64, 0_u64] {
        for offset in 0..messages_per_run as u64 {
            let time = run_start + offset;
            writer
                .write(&Message {
                    channel: channel.clone(),
                    sequence: time as u32,
                    log_time: time,
                    publish_time: time,
                    data: Cow::Owned(time.to_le_bytes().to_vec()),
                })
                .unwrap();
        }
    }
    writer.finish().unwrap();
    drop(writer);
    temp
}

/// Writes a valid indexed MCAP, then flips the first byte of the first
/// chunk's compressed data (the zstd frame magic) so that chunk fails to
/// decompress during streaming while the summary stays intact.
pub(crate) fn write_corrupt_chunk_mcap() -> NamedTempFile {
    let temp = write_mcap(true, Some(Compression::Zstd), 64, 512);
    let mut contents = std::fs::read(temp.path()).unwrap();
    let summary = crate::parse_summary_from_bytes(&bytes::Bytes::from(contents.clone()))
        .unwrap()
        .expect("fixture has a summary");
    let data_offset = summary.chunk_indexes[0]
        .compressed_data_offset()
        .expect("fixture chunk has a data offset");
    contents[usize::try_from(data_offset).unwrap()] ^= 0xFF;
    std::fs::write(temp.path(), &contents).unwrap();
    temp
}

pub(crate) async fn make_reader(
    file: &NamedTempFile,
    options: McapReadOptions,
) -> DaftResult<(NativeMcapReader, daft_io::IOStatsRef)> {
    let io_client = get_io_client(true, Arc::new(IOConfig::default()))?;
    let io_stats = IOStatsContext::new("daft-mcap unit test");
    let reader = NativeMcapReader::new(
        file.path().to_string_lossy(),
        io_client,
        io_stats.clone(),
        options,
    )
    .await?;
    Ok((reader, io_stats))
}

pub(crate) async fn collect_rows(
    reader: &mut NativeMcapReader,
) -> DaftResult<Vec<(String, u64, Vec<u8>)>> {
    let mut rows = Vec::new();
    while let Some(batch) = reader.next_batch().await? {
        let topics = batch.get_column(1).utf8()?;
        let log_times = batch.get_column(2).u64()?;
        let payloads = batch.get_column(5).binary()?;
        for index in 0..batch.len() {
            rows.push((
                topics.get(index).unwrap().to_string(),
                log_times.get(index).unwrap(),
                payloads.get(index).unwrap().to_vec(),
            ));
        }
    }
    Ok(rows)
}

pub(crate) async fn collect_stream(
    file: &NamedTempFile,
    read_options: McapReadOptions,
    convert_options: McapConvertOptions,
) -> DaftResult<Vec<daft_recordbatch::RecordBatch>> {
    let io_client = get_io_client(true, Arc::new(IOConfig::default()))?;
    let io_stats = IOStatsContext::new("daft-mcap stream unit test");
    let uri = file.path().to_string_lossy().into_owned();
    stream_mcap(&uri, io_client, io_stats, read_options, convert_options)
        .await?
        .try_collect()
        .await
}
