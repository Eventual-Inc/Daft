//! Reading one output partition straight off the shared mount.
//!
//! This is the counterpart to the Flight `do_get` path: same combined files, same
//! byte ranges, same raw-IPC forwarding — but resolved locally from the file's own
//! index instead of from the writer's in-memory
//! [`crate::shuffle_cache::PartitionCache`]. That independence from the writer
//! process is the whole point: it removes the proxy hop while the writer is alive,
//! and it keeps the data readable once the writer is gone.
//!
//! Every range read is verified before it is trusted: the parser must consume
//! exactly the indexed byte count, and the bytes must match the indexed CRC-32.
//! A file that is short, holed, or otherwise not what the writer committed is
//! reported as an error rather than decoded into plausible-looking rows.

use std::{
    io::SeekFrom,
    pin::Pin,
    task::{Context, Poll},
};

use arrow_flight::{FlightData, SchemaAsIpc, decode::FlightRecordBatchStream};
use arrow_ipc::writer::IpcWriteOptions;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader, ReadBuf, Take},
};

use super::{index, shared_map_file};
use crate::{
    client::flight_client::FlightRecordBatchStreamToDaftRecordBatchStream,
    server::stream::next_flight_data,
};

/// One map input a reducer must gather: which task wrote it, and which attempt
/// of that task the coordinator selected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MapInput {
    pub input_id: u32,
    pub attempt: u64,
}

/// `AsyncRead` adapter that folds every byte it hands out into a CRC-32.
///
/// Sits *below* the `BufReader`, so it hashes what the buffer pulled from the
/// file. That is exactly the indexed range when the parser consumed everything
/// (the `Take` stops it there), and the completeness check rejects the read
/// before the CRC is consulted when it did not.
struct HashingReader<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: AsyncRead + Unpin> AsyncRead for HashingReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(&poll, Poll::Ready(Ok(()))) {
            this.hasher.update(&buf.filled()[before..]);
        }
        poll
    }
}

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

/// Stream the raw IPC messages of one output partition out of one map file,
/// verifying the range against the index before the stream ends.
fn read_one_map_file(
    path: String,
    partition_idx: usize,
) -> BoxStream<'static, DaftResult<FlightData>> {
    Box::pin(async_stream::try_stream! {
        // TODO: recompute from lineage instead of failing. A missing file here
        // means the selected map attempt died before its commit rename landed, so
        // there is no copy anywhere and the only correct recovery is to re-run
        // that map task. Flotilla has no lineage-recompute path today, so the
        // query fails.
        let mut file = File::open(&path).await.map_err(|e| {
            DaftError::External(
                format!("Failed to open shared shuffle map file {}: {}", path, e).into(),
            )
        })?;
        let region = read_index_region(&mut file, &path).await?;
        let entry = index::partition_entry(&region, partition_idx, &path)?;

        // An empty output partition contributes no IPC messages at all.
        if entry.is_empty() {
            return;
        }

        file.seek(SeekFrom::Start(entry.start)).await.map_err(DaftError::IoError)?;
        let mut reader = BufReader::new(HashingReader {
            inner: file.take(entry.len()),
            hasher: crc32fast::Hasher::new(),
        });

        // The messages are forwarded as they are parsed, so a corrupt range can
        // already have had earlier messages consumed downstream by the time it is
        // detected. That is acceptable because detection fails the read — and so
        // the task — rather than letting partial output stand as complete.
        while let Some(message) = next_flight_data(&mut reader).await? {
            yield message;
        }

        verify_range(&reader, &entry, partition_idx, &path)?;
    })
}

/// Confirm the parser consumed the whole indexed range and that its bytes match
/// the indexed checksum.
fn verify_range(
    reader: &BufReader<HashingReader<Take<File>>>,
    entry: &index::PartitionEntry,
    partition_idx: usize,
    path: &str,
) -> DaftResult<()> {
    // Bytes the file still owes plus bytes buffered but never parsed: either one
    // means the stream ended early — a short file, or a hole read as end-of-stream.
    let unconsumed = reader.get_ref().inner.limit() + reader.buffer().len() as u64;
    if unconsumed != 0 {
        return Err(DaftError::InternalError(format!(
            "shuffle map file {} partition {} is incomplete: {} of {} bytes unread \
             (the writer may have died before its data reached shared storage)",
            path,
            partition_idx,
            unconsumed,
            entry.len()
        )));
    }
    let actual = reader.get_ref().hasher.clone().finalize();
    if actual != entry.crc32 {
        return Err(DaftError::InternalError(format!(
            "shuffle map file {} partition {} failed checksum: expected {:#010x}, got {:#010x} \
             (the file is not what its writer committed)",
            path, partition_idx, entry.crc32, actual
        )));
    }
    Ok(())
}

/// Read output partition `partition_idx` of `shuffle_id` from the shared mount,
/// gathering the contributions of every listed map input.
///
/// Map files are read unordered and concurrently: a reduce task's inputs are
/// independent and the shuffle is already an unordered exchange, so there is
/// nothing to gain from reading them in sequence and a great deal of latency to
/// lose. The batches of all files are decoded as one logical IPC stream, which
/// costs a single schema decode rather than one per file. Interleaving messages
/// from different files is sound because the stream carries only record batches:
/// Daft has no dictionary-encoded type, so there is no per-file dictionary state
/// for the decoder to confuse.
pub fn read_partition_stream(
    shared_root: &str,
    shuffle_id: u64,
    inputs: &[MapInput],
    partition_idx: u32,
    schema: SchemaRef,
    concurrency: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let paths = inputs
        .iter()
        .map(|input| shared_map_file(shared_root, shuffle_id, input.input_id, input.attempt))
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
    use std::{
        io::{Read, Seek, Write},
        sync::Arc,
    };

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
    use crate::{
        oneshot_writer::{OneShotTarget, write_partitions_one_shot},
        store::ShuffleDurability,
    };

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
    fn partitions(schema: &SchemaRef, first: usize) -> Vec<MicroPartition> {
        vec![
            make_dummy_mp(first),
            MicroPartition::empty(Some(schema.clone())),
            make_dummy_mp(300),
        ]
    }

    async fn read_rows(
        root: &str,
        shuffle_id: u64,
        inputs: &[MapInput],
        partition_idx: u32,
        schema: SchemaRef,
    ) -> DaftResult<Vec<u8>> {
        let batches: Vec<RecordBatch> =
            read_partition_stream(root, shuffle_id, inputs, partition_idx, schema, 4)?
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
        input: MapInput,
        schema: SchemaRef,
        compression: Option<arrow_ipc::CompressionType>,
        first_partition_rows: usize,
    ) -> DaftResult<()> {
        write_partitions_one_shot(
            input.input_id,
            shuffle_id,
            input.attempt,
            OneShotTarget::Shared {
                shared_root: root.to_string(),
                durability: ShuffleDurability::None,
            },
            schema.clone(),
            compression,
            partitions(&schema, first_partition_rows),
        )
        .await?;
        Ok(())
    }

    fn expected(n: usize) -> Vec<u8> {
        (0..n).map(|i| i as u8).collect()
    }

    #[tokio::test]
    async fn shared_round_trip_preserves_each_partition() -> DaftResult<()> {
        let dir = tempdir("basic");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 7,
            attempt: 0x77,
        };
        write_shared(root, 42, input, schema.clone(), None, 50).await?;

        assert_eq!(
            read_rows(root, 42, &[input], 0, schema.clone()).await?,
            expected(50)
        );
        // The empty partition is a zero-length range, not a gap.
        assert!(
            read_rows(root, 42, &[input], 1, schema.clone())
                .await?
                .is_empty()
        );
        assert_eq!(
            read_rows(root, 42, &[input], 2, schema.clone()).await?,
            expected(300)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn shared_round_trip_survives_ipc_compression() -> DaftResult<()> {
        let dir = tempdir("lz4");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 0,
            attempt: 1,
        };
        write_shared(
            root,
            1,
            input,
            schema.clone(),
            Some(arrow_ipc::CompressionType::LZ4_FRAME),
            50,
        )
        .await?;

        assert_eq!(
            read_rows(root, 1, &[input], 2, schema.clone()).await?,
            expected(300)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn one_partition_gathers_every_map_input() -> DaftResult<()> {
        let dir = tempdir("fanin");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        // Three map tasks, sharded across different subdirectories.
        let inputs = [1u32, 2, 257].map(|input_id| MapInput {
            input_id,
            attempt: 9,
        });
        for input in inputs {
            write_shared(root, 9, input, schema.clone(), None, 50).await?;
        }

        let mut rows = read_rows(root, 9, &inputs, 0, schema.clone()).await?;
        rows.sort_unstable();
        let mut want = expected(50)
            .into_iter()
            .cycle()
            .take(150)
            .collect::<Vec<_>>();
        want.sort_unstable();
        assert_eq!(rows, want);

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// Two attempts of one map task, with *different* contents (as they would be
    /// under random repartitioning), both committed. The reader must return only
    /// the attempt it was asked for — never the other, never a mix.
    #[tokio::test]
    async fn reader_addresses_exactly_the_selected_attempt() -> DaftResult<()> {
        let dir = tempdir("attempts");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let losing = MapInput {
            input_id: 3,
            attempt: 0xaaaa,
        };
        let selected = MapInput {
            input_id: 3,
            attempt: 0xbbbb,
        };
        write_shared(root, 5, losing, schema.clone(), None, 40).await?;
        write_shared(root, 5, selected, schema.clone(), None, 60).await?;

        assert_eq!(
            read_rows(root, 5, &[selected], 0, schema.clone()).await?,
            expected(60)
        );
        assert_eq!(
            read_rows(root, 5, &[losing], 0, schema.clone()).await?,
            expected(40)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn missing_map_file_reports_its_path() -> DaftResult<()> {
        let dir = tempdir("missing");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 404,
            attempt: 0xdead,
        };
        let err = read_rows(root, 3, &[input], 0, schema).await.unwrap_err();
        assert!(
            err.to_string().contains("map_404_000000000000dead.arrow"),
            "error should name the missing file, got: {}",
            err
        );
        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// Locate the committed file and return (path, partition-2 entry).
    async fn committed_file(root: &str, input: MapInput) -> (String, index::PartitionEntry) {
        let path = shared_map_file(root, 77, input.input_id, input.attempt);
        let mut f = std::fs::File::open(&path).unwrap();
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes).unwrap();
        let entry = index::partition_entry(&bytes, 2, &path).unwrap();
        (path, entry)
    }

    /// A writer that died before its data reached the mount leaves a file whose
    /// tail is missing. That must read as an error, not as fewer rows.
    #[tokio::test]
    async fn truncated_file_is_an_error_not_fewer_rows() -> DaftResult<()> {
        let dir = tempdir("truncated");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 1,
            attempt: 1,
        };
        write_shared(root, 77, input, schema.clone(), None, 50).await?;
        let (path, entry) = committed_file(root, input).await;

        // Cut the file in the middle of partition 2's range.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(entry.start + entry.len() / 2).unwrap();

        let err = read_rows(root, 77, &[input], 2, schema.clone())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("incomplete") || err.contains("early eof") || err.contains("checksum"),
            "truncation must surface as an error, got: {}",
            err
        );
        // Partitions before the cut are unaffected.
        assert_eq!(
            read_rows(root, 77, &[input], 0, schema).await?,
            expected(50)
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// A hole where data should be — pages that never reached the mount — decodes
    /// as zeros, which look like a valid (if odd) record batch body. Only the
    /// checksum catches it.
    #[tokio::test]
    async fn zeroed_hole_inside_a_message_fails_the_checksum() -> DaftResult<()> {
        let dir = tempdir("hole");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 2,
            attempt: 2,
        };
        write_shared(root, 77, input, schema.clone(), None, 50).await?;
        let (path, entry) = committed_file(root, input).await;

        // Zero the last 8 *non-zero* bytes of the range. The body ends in
        // alignment padding that is already zero, so a fixed offset from the end
        // could land on bytes a hole would not change; the last non-zero byte is
        // real data inside the final message body, past all IPC framing, so only
        // values change and the parser stays happy.
        let mut bytes = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        let range = &bytes[entry.start as usize..entry.end as usize];
        let last_nonzero = range.iter().rposition(|b| *b != 0).unwrap();
        assert!(
            last_nonzero >= 8,
            "range too small for the test to be meaningful"
        );
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(entry.start + (last_nonzero as u64) - 7))
            .unwrap();
        f.write_all(&[0u8; 8]).unwrap();
        drop(f);

        let err = read_rows(root, 77, &[input], 2, schema)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("checksum"),
            "expected a checksum failure, got: {}",
            err
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }

    /// A hole that lands on a message boundary reads as end-of-stream, which the
    /// byte count catches even before the checksum is consulted.
    #[tokio::test]
    async fn zeroed_message_header_is_detected_as_incomplete() -> DaftResult<()> {
        let dir = tempdir("hole_boundary");
        let root = dir.to_str().unwrap();
        let schema = dummy_schema();
        let input = MapInput {
            input_id: 4,
            attempt: 4,
        };
        write_shared(root, 77, input, schema.clone(), None, 50).await?;
        let (path, entry) = committed_file(root, input).await;

        // Zero the first message's framing so the parser sees a zero length.
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(entry.start)).unwrap();
        f.write_all(&[0u8; 8]).unwrap();
        drop(f);

        let err = read_rows(root, 77, &[input], 2, schema)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("incomplete"),
            "expected an incompleteness error, got: {}",
            err
        );

        std::fs::remove_dir_all(&dir).unwrap();
        Ok(())
    }
}
