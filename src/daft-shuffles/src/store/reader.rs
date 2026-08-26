//! Reading one output partition straight off the shared mount.
//!
//! This is the counterpart to the Flight `do_get` path: same combined files, same
//! byte ranges, same raw-IPC forwarding — but resolved locally from the file's own
//! index instead of from the writer's in-memory
//! [`crate::shuffle_cache::PartitionCache`]. That independence from the writer
//! process is the whole point: it removes the proxy hop while the writer is alive,
//! and it keeps the data readable once the writer is gone.

use std::io::SeekFrom;

use arrow_flight::{FlightData, SchemaAsIpc, decode::FlightRecordBatchStream};
use arrow_ipc::writer::IpcWriteOptions;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
};

use super::{index, shared_map_file};
use crate::{
    client::flight_client::FlightRecordBatchStreamToDaftRecordBatchStream,
    server::stream::FlightDataStreamReader,
};

/// How many map files to have open and in flight at once.
///
/// Per-file cost is dominated by round trips rather than bytes — an open plus an
/// index read is ~3 ms on the reference Lustre mount regardless of how much data
/// follows — so this needs to be well above the scan-task default of 8 to keep a
/// reduce task with thousands of map inputs from serializing on latency. Each slot
/// can hold roughly one IPC chunk in memory, which bounds the memory cost.
pub const DEFAULT_SHARED_READ_CONCURRENCY: usize = 16;

/// Read the index region of an already-open map file.
///
/// Takes one speculative read sized to cover the common case and only issues a
/// second when the shuffle has more partitions than that covers.
async fn read_index_region(file: &mut File, path: &str) -> DaftResult<Vec<u8>> {
    let mut probe = vec![0u8; index::PROBE_BYTES];
    let mut filled = 0;
    while filled < probe.len() {
        let n = file.read(&mut probe[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    probe.truncate(filled);

    let num_partitions = index::parse_num_partitions(&probe, path)?;
    let needed = index::index_region_bytes(num_partitions);
    if probe.len() >= needed {
        probe.truncate(needed);
        return Ok(probe);
    }

    probe.resize(needed, 0);
    file.read_exact(&mut probe[filled..]).await?;
    Ok(probe)
}

/// Stream the raw IPC messages of one output partition out of one map file.
fn read_one_map_file(
    path: String,
    partition_idx: usize,
) -> BoxStream<'static, DaftResult<FlightData>> {
    Box::pin(async_stream::try_stream! {
        // TODO: recompute from lineage instead of failing. A missing file here
        // means the map attempt died before its commit rename landed, so there is
        // no copy anywhere and the only correct recovery is to re-run that map
        // task. Flotilla has no lineage-recompute path today, so the query fails.
        let mut file = File::open(&path).await.map_err(|e| {
            DaftError::External(
                format!("Failed to open shared shuffle map file {}: {}", path, e).into(),
            )
        })?;
        let region = read_index_region(&mut file, &path).await?;
        let (start, end) = index::partition_range(&region, partition_idx, &path)?;

        // An empty output partition contributes no IPC messages at all.
        if end > start {
            file.seek(SeekFrom::Start(start)).await.map_err(DaftError::IoError)?;
            let limited = file.take(end - start);
            let reader = FlightDataStreamReader::from_skipped(BufReader::new(limited));
            let inner = reader.into_stream();
            futures::pin_mut!(inner);
            while let Some(item) = inner.next().await {
                yield item?;
            }
        }
    })
}

/// Read output partition `partition_idx` of `shuffle_id` from the shared mount,
/// gathering the contributions of every listed map input.
///
/// Map files are read unordered and concurrently: a reduce task's inputs are
/// independent and the shuffle is already an unordered exchange, so there is
/// nothing to gain from reading them in sequence and a great deal of latency to
/// lose. The batches of all files are decoded as one logical IPC stream, which
/// costs a single schema decode rather than one per file.
pub fn read_partition_stream(
    shared_root: &str,
    shuffle_id: u64,
    input_ids: &[u32],
    partition_idx: u32,
    schema: SchemaRef,
    concurrency: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let paths = input_ids
        .iter()
        .map(|input_id| shared_map_file(shared_root, shuffle_id, *input_id))
        .collect::<Vec<_>>();

    let arrow_schema = schema.to_arrow()?;
    let flight_schema: FlightData =
        SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default()).into();

    let partition_idx = partition_idx as usize;
    let data = futures::stream::iter(paths)
        .flat_map_unordered(Some(concurrency.max(1)), move |path| {
            read_one_map_file(path, partition_idx)
        })
        .map(|item| item.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e))));

    let flight_data = futures::stream::once(async move { Ok(flight_schema) }).chain(data);
    let arrow_stream = FlightRecordBatchStream::new_from_flight_data(flight_data);
    Ok(FlightRecordBatchStreamToDaftRecordBatchStream::new(arrow_stream, schema).boxed())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_micropartition::MicroPartition;
    use daft_schema::{
        dtype::DataType,
        field::Field,
        schema::{Schema, SchemaRef},
    };
    use daft_writers::test::make_dummy_mp;
    use futures::TryStreamExt;

    use super::*;
    use crate::oneshot_writer::{OneShotTarget, write_partitions_one_shot};

    fn dummy_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("ints", DataType::UInt8)]))
    }

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "daft_shared_roundtrip_{}_{}_{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `make_dummy_mp(n)` yields `n` rows holding `0..n` truncated to u8.
    fn partitions(schema: &SchemaRef) -> Vec<MicroPartition> {
        vec![
            make_dummy_mp(50),
            MicroPartition::empty(Some(schema.clone())),
            make_dummy_mp(300),
        ]
    }

    async fn read_rows(
        root: &str,
        shuffle_id: u64,
        input_ids: &[u32],
        partition_idx: u32,
        schema: SchemaRef,
    ) -> DaftResult<Vec<u8>> {
        let batches: Vec<RecordBatch> =
            read_partition_stream(root, shuffle_id, input_ids, partition_idx, schema, 4)?
                .try_collect()
                .await?;
        let mut out = Vec::new();
        for batch in batches {
            let series = batch.get_column(0);
            let arr = series.u8().unwrap();
            for i in 0..arr.len() {
                out.push(arr.get(i).unwrap());
            }
        }
        Ok(out)
    }

    async fn write_shared(
        root: &str,
        shuffle_id: u64,
        input_id: u32,
        schema: SchemaRef,
        compression: Option<arrow_ipc::CompressionType>,
    ) -> DaftResult<()> {
        write_partitions_one_shot(
            input_id,
            shuffle_id,
            OneShotTarget::Shared {
                shared_root: root.to_string(),
                durability: super::super::ShuffleDurability::None,
            },
            schema.clone(),
            compression,
            partitions(&schema),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn shared_round_trip_preserves_each_partition() -> DaftResult<()> {
        let dir = tempdir("basic");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        write_shared(root, 42, 7, schema.clone(), None).await?;

        let p0 = read_rows(root, 42, &[7], 0, schema.clone()).await?;
        assert_eq!(p0, (0..50).map(|i| i as u8).collect::<Vec<_>>());

        // The empty partition is a zero-length range, not a gap.
        let p1 = read_rows(root, 42, &[7], 1, schema.clone()).await?;
        assert!(p1.is_empty());

        let p2 = read_rows(root, 42, &[7], 2, schema.clone()).await?;
        assert_eq!(p2, (0..300).map(|i| i as u8).collect::<Vec<_>>());

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn shared_round_trip_survives_ipc_compression() -> DaftResult<()> {
        let dir = tempdir("lz4");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        write_shared(
            root,
            1,
            0,
            schema.clone(),
            Some(arrow_ipc::CompressionType::LZ4_FRAME),
        )
        .await?;

        let p2 = read_rows(root, 1, &[0], 2, schema.clone()).await?;
        assert_eq!(p2, (0..300).map(|i| i as u8).collect::<Vec<_>>());

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn one_partition_gathers_every_map_input() -> DaftResult<()> {
        let dir = tempdir("fanin");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        // Three map tasks, sharded across different subdirectories.
        for input_id in [1u32, 2, 257] {
            write_shared(root, 9, input_id, schema.clone(), None).await?;
        }

        let mut rows = read_rows(root, 9, &[1, 2, 257], 0, schema.clone()).await?;
        rows.sort_unstable();
        let mut expected = (0..50)
            .map(|i| i as u8)
            .cycle()
            .take(150)
            .collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(rows, expected);

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn missing_map_file_reports_its_path() -> DaftResult<()> {
        let dir = tempdir("missing");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let err = read_rows(root, 3, &[404], 0, schema).await.unwrap_err();
        assert!(
            err.to_string().contains("map_404.arrow"),
            "error should name the missing file, got: {}",
            err
        );
        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }
}
