use std::{borrow::Cow, collections::BTreeMap, fs::File, io::BufWriter, sync::Arc};

use common_error::DaftResult;
use daft_io::{IOConfig, IOStatsContext, get_io_client};
use mcap::{Channel, Compression, Message, WriteOptions};
use tempfile::NamedTempFile;

use crate::{McapReadOptions, NativeMcapReader};

fn write_mcap(
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

async fn make_reader(
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

async fn collect_rows(reader: &mut NativeMcapReader) -> DaftResult<Vec<(String, u64, Vec<u8>)>> {
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

#[tokio::test]
async fn unfiltered_reader_streams_file_once() -> DaftResult<()> {
    let file = write_mcap(true, None, 20, 32);
    let file_size = file.as_file().metadata().unwrap().len() as usize;
    let (mut reader, io_stats) = make_reader(&file, McapReadOptions::default()).await?;

    assert!(!reader.indexed());
    assert_eq!(collect_rows(&mut reader).await?.len(), 20);
    drop(reader);
    assert_eq!(io_stats.load_bytes_read(), file_size);
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
async fn invalid_magic_is_rejected() -> DaftResult<()> {
    let file = NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"not an mcap file").unwrap();
    let (mut reader, _) = make_reader(&file, McapReadOptions::default()).await?;
    let result = reader.next_batch().await;
    assert!(
        result
            .err()
            .is_some_and(|error| error.to_string().to_lowercase().contains("magic"))
    );
    Ok(())
}
