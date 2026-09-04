//! Measurement harness for the shared-read index probe.
//!
//! Not a correctness test: it exists so that decisions about the on-disk index
//! are made against numbers rather than arithmetic. Written for the question
//! "is the 128 KiB probe per (map file x reduce task) worth designing away", and
//! kept because the same question has to be re-asked on a real shared mount,
//! where the cost of a round trip and the cost of a byte are nothing like they
//! are on local NVMe.
//!
//! ```text
//! cargo test -p daft-shuffles --release bench_shared_read -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Reports, per phase: wall time, bytes requested from the kernel (`rchar`), and
//! bytes the kernel actually fetched from the device (`read_bytes`). The gap
//! between the last two is the page cache, which is what decides whether an
//! amplification in the first costs anything at all.

use std::{io::SeekFrom, time::Instant};

use daft_recordbatch::RecordBatch;
use daft_writers::test::make_dummy_mp;
use futures::TryStreamExt;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use super::{
    ShuffleDurability, index,
    reader::{MapInput, read_partition_stream, tests::dummy_schema},
    shared_map_file,
};
use crate::oneshot_writer::{OneShotTarget, write_partitions_one_shot};

/// `rchar` and `read_bytes` from `/proc/self/io`: bytes this process asked the
/// kernel for, and bytes the kernel actually fetched from the block device.
/// The gap between them is the page cache, which is exactly the thing that
/// decides whether this amplification costs anything in practice.
fn io_counters() -> (u64, u64) {
    let text = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    let field = |name: &str| -> u64 {
        text.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    };
    (field("rchar:"), field("read_bytes:"))
}

fn drop_page_cache() {
    let _ = std::fs::File::open("/").map(|_| ());
    std::process::Command::new("sync").status().ok();
    if std::fs::write("/proc/sys/vm/drop_caches", "3").is_err() {
        eprintln!("  (warning: could not drop page cache; cold numbers are not cold)");
    }
}

struct Phase {
    label: &'static str,
    elapsed_ms: u128,
    rchar: u64,
    read_bytes: u64,
}

/// Run `f` `reps` times and keep the fastest.
///
/// The minimum rather than the mean because the question is what an access
/// pattern costs, and every source of noise on a shared machine only ever adds
/// time. Byte counts are per-rep and identical across reps by construction.
///
/// A cold rep drops the page cache first, so `reps` is forced to 1 there — the
/// second rep would not be cold.
async fn timed<F, Fut>(label: &'static str, cold: bool, reps: usize, f: F) -> Phase
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let reps = if cold { 1 } else { reps.max(1) };
    let mut best_ms = u128::MAX;
    let (mut rchar, mut read_bytes) = (0, 0);
    for _ in 0..reps {
        if cold {
            drop_page_cache();
        }
        let (rchar0, rb0) = io_counters();
        let start = Instant::now();
        f().await;
        let elapsed = start.elapsed().as_millis();
        let (rchar1, rb1) = io_counters();
        best_ms = best_ms.min(elapsed);
        rchar = rchar1 - rchar0;
        read_bytes = rb1 - rb0;
    }
    Phase {
        label,
        elapsed_ms: best_ms,
        rchar,
        read_bytes,
    }
}

/// Unhinted: one speculative 128 KiB read covering the whole index region. What
/// the reader does for the first map file of a shuffle, and what it did for every
/// file before the partition count was remembered.
async fn resolve_current(path: &str, partition_idx: usize) -> index::PartitionEntry {
    let mut file = File::open(path).await.unwrap();
    let (region, _) = super::reader::read_index_region(&mut file, path, None)
        .await
        .unwrap();
    index::partition_entry(&region, partition_idx, path).unwrap()
}

/// What the current *format* allows without changing it: read the prefix, then
/// the two offsets, then the checksum — three small reads at computed offsets,
/// separate because the layout puts offsets and checksums in different arrays.
async fn resolve_targeted_v2(path: &str, partition_idx: usize) -> index::PartitionEntry {
    let mut file = File::open(path).await.unwrap();
    let mut prefix = [0u8; index::PREFIX_BYTES];
    file.read_exact(&mut prefix).await.unwrap();
    let n = index::parse_num_partitions(&prefix, path).unwrap();

    let mut offsets = [0u8; 16];
    file.seek(SeekFrom::Start(
        (index::PREFIX_BYTES + 8 * partition_idx) as u64,
    ))
    .await
    .unwrap();
    file.read_exact(&mut offsets).await.unwrap();

    let mut crc = [0u8; 4];
    file.seek(SeekFrom::Start(
        (index::PREFIX_BYTES + 8 * (n + 1) + 4 * partition_idx) as u64,
    ))
    .await
    .unwrap();
    file.read_exact(&mut crc).await.unwrap();

    index::PartitionEntry {
        start: u64::from_le_bytes(offsets[..8].try_into().unwrap()),
        end: u64::from_le_bytes(offsets[8..].try_into().unwrap()),
        crc32: u32::from_le_bytes(crc),
    }
}

/// Hinted: the same single read, sized to the index that is actually there. What
/// the reader does for every map file after the first.
async fn resolve_rightsized(path: &str, partition_idx: usize, n: usize) -> index::PartitionEntry {
    let mut file = File::open(path).await.unwrap();
    let (region, _) = super::reader::read_index_region(&mut file, path, Some(n))
        .await
        .unwrap();
    index::partition_entry(&region, partition_idx, path).unwrap()
}

/// What an interleaved `[start, crc][start, crc]...[end]` layout would allow,
/// with `num_partitions` remembered per shuffle (every map file of one shuffle
/// has the same count): a single 20-byte read at a computed offset.
async fn resolve_targeted_v3(path: &str, partition_idx: usize, n: usize) -> index::PartitionEntry {
    let mut file = File::open(path).await.unwrap();
    let mut entry = [0u8; 20];
    // Exactly where an interleaved `[start, crc]` array would put partition
    // `p`'s entry and the next entry's start. In a v2 file those bytes mean
    // something else, which is fine — what is being measured is the access
    // pattern's cost, not its result. `16 + 12(n-1) + 20` is the v2 region
    // size, so this never reads past the index.
    debug_assert!(partition_idx < n);
    let at = (index::PREFIX_BYTES + 12 * partition_idx) as u64;
    file.seek(SeekFrom::Start(at)).await.unwrap();
    file.read_exact(&mut entry).await.unwrap();
    index::PartitionEntry {
        start: 0,
        end: 0,
        crc32: 0,
    }
}

async fn write_shape(root: &str, shuffle_id: u64, m: usize, p: usize, cell_bytes: usize) -> u64 {
    let schema = dummy_schema();
    let mut total = 0u64;
    for input_id in 0..m {
        let partitions = (0..p)
            .map(|_| make_dummy_mp(cell_bytes))
            .collect::<Vec<_>>();
        write_partitions_one_shot(
            input_id as u32,
            shuffle_id,
            input_id as u64,
            OneShotTarget::Shared {
                shared_root: root.to_string(),
                durability: ShuffleDurability::None,
            },
            schema.clone(),
            None,
            partitions,
        )
        .await
        .unwrap();
        let path = shared_map_file(root, shuffle_id, input_id as u32, input_id as u64);
        total += std::fs::metadata(&path).unwrap().len();
    }
    total
}

async fn run_shape(name: &str, m: usize, p: usize, cell_bytes: usize) {
    let dir = std::env::temp_dir().join(format!("daft_bench_{}_{}", name, std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let root = dir.to_str().unwrap().to_string();
    let shuffle_id = 0xBEEFu64;

    let on_disk = write_shape(&root, shuffle_id, m, p, cell_bytes).await;
    let paths = (0..m)
        .map(|i| shared_map_file(&root, shuffle_id, i as u32, i as u64))
        .collect::<Vec<_>>();
    let inputs = (0..m)
        .map(|i| MapInput {
            input_id: i as u32,
            attempt: i as u64,
        })
        .collect::<Vec<_>>();
    let schema = dummy_schema();
    let pairs = (m * p) as u64;

    println!(
        "\n=== {name}: {m} map files x {p} partitions x {cell_bytes} B/cell \
         ({} MiB on disk, {pairs} file-reads per full shuffle) ===",
        on_disk / (1024 * 1024)
    );

    let mut phases = Vec::new();

    // Everything one shuffle's reduce side does, as it does it today.
    for (label, cold) in [("e2e current (cold)", true), ("e2e current (warm)", false)] {
        let (root, inputs, schema) = (root.clone(), inputs.clone(), schema.clone());
        phases.push(
            timed(label, cold, 3, || {
                let (root, inputs, schema) = (root.clone(), inputs.clone(), schema.clone());
                async move {
                    for partition_idx in 0..p {
                        let stream = read_partition_stream(
                            &root,
                            shuffle_id,
                            &inputs,
                            partition_idx as u32,
                            schema.clone(),
                            16,
                        )
                        .unwrap();
                        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
                        std::hint::black_box(batches.len());
                    }
                }
            })
            .await,
        );
    }

    // Index resolution alone, so the amplification can be separated from the
    // data it is amplifying.
    for (label, cold) in [
        ("index unhinted (cold)", true),
        ("index unhinted (warm)", false),
    ] {
        let paths = paths.clone();
        phases.push(
            timed(label, cold, 5, || {
                let paths = paths.clone();
                async move {
                    for partition_idx in 0..p {
                        for path in &paths {
                            std::hint::black_box(resolve_current(path, partition_idx).await);
                        }
                    }
                }
            })
            .await,
        );
    }
    for (label, cold) in [
        ("index v2-targeted (cold)", true),
        ("index v2-targeted (warm)", false),
    ] {
        let paths = paths.clone();
        phases.push(
            timed(label, cold, 5, || {
                let paths = paths.clone();
                async move {
                    for partition_idx in 0..p {
                        for path in &paths {
                            std::hint::black_box(resolve_targeted_v2(path, partition_idx).await);
                        }
                    }
                }
            })
            .await,
        );
    }
    for (label, cold) in [
        ("index hinted (cold)", true),
        ("index hinted (warm)", false),
    ] {
        let paths = paths.clone();
        phases.push(
            timed(label, cold, 5, || {
                let paths = paths.clone();
                async move {
                    for partition_idx in 0..p {
                        for path in &paths {
                            std::hint::black_box(resolve_rightsized(path, partition_idx, p).await);
                        }
                    }
                }
            })
            .await,
        );
    }
    for (label, cold) in [
        ("index v3-targeted (cold)", true),
        ("index v3-targeted (warm)", false),
    ] {
        let paths = paths.clone();
        phases.push(
            timed(label, cold, 5, || {
                let paths = paths.clone();
                async move {
                    for partition_idx in 0..p {
                        for path in &paths {
                            std::hint::black_box(resolve_targeted_v3(path, partition_idx, p).await);
                        }
                    }
                }
            })
            .await,
        );
    }

    println!(
        "{:<26} {:>10} {:>14} {:>14} {:>12}",
        "phase", "best ms", "syscall MiB", "disk MiB", "B/file-read"
    );
    for ph in &phases {
        println!(
            "{:<26} {:>10} {:>14.1} {:>14.1} {:>12}",
            ph.label,
            ph.elapsed_ms,
            ph.rchar as f64 / (1024.0 * 1024.0),
            ph.read_bytes as f64 / (1024.0 * 1024.0),
            ph.rchar / pairs.max(1),
        );
    }

    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run with --ignored --nocapture --test-threads=1"]
async fn bench_shared_read_index_amplification() {
    run_shape("wide-small-cells", 32, 512, 2 * 1024).await;
    run_shape("square-medium-cells", 64, 64, 32 * 1024).await;
    run_shape("square-large-cells", 32, 32, 512 * 1024).await;
}
