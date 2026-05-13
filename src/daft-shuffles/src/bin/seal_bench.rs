//! Benchmark for the production Flight shuffle path:
//!   synthetic MicroPartitions -> partition_by_hash -> {oneshot, append, multi_file}
//!   -> ShuffleFlightServer (register) -> optional seal -> FlightClientManager (read)
//!
//! Unlike `shuffle_bench` (which tests `InProgressShuffleCache` + `MultiPartitionShuffleCache`),
//! this drives the writers exposed at `daft_shuffles::{oneshot_writer, multi_file_writer,
//! partition_file_writer}` — i.e. what `RepartitionSink::finalize` actually calls today.
//!
//! Run:
//!   cargo run --release -p daft-shuffles --bin seal_bench -- \
//!     --writer oneshot --outputs 512 --bytes 4GiB --seal never
//!
//! Sweep:
//!   for writer in oneshot append multi_file; do
//!     for seal in never always; do
//!       cargo run --release -p daft-shuffles --bin seal_bench -- \
//!         --writer $writer --seal $seal --outputs 512 --bytes 4GiB
//!     done
//!   done

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema, UInt64Array},
    series::{IntoSeries, Series},
};
use daft_dsl::expr::{bound_col, bound_expr::BoundExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_shuffles::{
    client::FlightClientManager,
    multi_partition_cache::{log_write_agg_summary, write_agg_snapshot},
    oneshot_writer::write_partitions_one_shot,
    parse_flight_compression,
    server::flight_server::{
        ShuffleFlightServer, log_read_agg_summary, read_agg_snapshot, start_server_loop,
    },
    shuffle_cache::{PartitionCache, partition_ref_id},
};
use futures::StreamExt;
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SealMode {
    Never,
    Always,
}

#[derive(Debug, Clone)]
struct BenchConfig {
    num_inputs: usize,
    num_outputs: usize,
    total_bytes: usize,
    rows_per_input: usize,
    payload_cols: usize,
    chunks_per_input: usize,
    map_concurrency: usize,
    read_concurrency: usize,
    shuffle_id: u64,
    shuffle_root: PathBuf,
    seal: SealMode,
    compression: Option<arrow_ipc::CompressionType>,
}

fn parse_args() -> BenchConfig {
    let mut num_inputs: usize = 100;
    let mut num_outputs: usize = 100;
    let mut total_bytes: usize = 1024 * 1024 * 1024;
    let mut payload_cols: usize = 4;
    let mut map_concurrency: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut read_concurrency: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut chunks_per_input: usize = 8;
    let mut seal = SealMode::Never;
    let mut compression_str = "none".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let val = args.next().unwrap_or_else(|| panic!("missing value for {arg}"));
        match arg.as_str() {
            "--inputs" => num_inputs = val.parse().unwrap(),
            "--outputs" => num_outputs = val.parse().unwrap(),
            "--bytes" => total_bytes = parse_size(&val),
            "--payload-cols" => payload_cols = val.parse().unwrap(),
            "--map-conc" => map_concurrency = val.parse().unwrap(),
            "--read-conc" => read_concurrency = val.parse().unwrap(),
            "--chunks-per-input" => chunks_per_input = val.parse().unwrap(),
            "--writer" => {
                if val != "oneshot" {
                    panic!("--writer must be 'oneshot' (got {val}); append/multi_file paths were removed");
                }
            }
            "--seal" => {
                seal = match val.as_str() {
                    "never" => SealMode::Never,
                    "always" => SealMode::Always,
                    other => panic!("unknown seal mode: {other} (expected never|always)"),
                }
            }
            "--compression" => compression_str = val,
            _ => panic!("unknown arg: {arg}"),
        }
    }

    let row_bytes = 8 * (1 + payload_cols);
    let total_rows = total_bytes / row_bytes;
    let rows_per_input = total_rows / num_inputs;
    let total_bytes_actual = rows_per_input * num_inputs * row_bytes;
    let compression = parse_flight_compression(Some(compression_str.as_str()))
        .expect("invalid --compression");

    let shuffle_id = 1u64;
    let shuffle_root = std::env::temp_dir().join(format!(
        "seal_bench_{}_{}_{}",
        num_inputs,
        num_outputs,
        std::process::id()
    ));

    BenchConfig {
        num_inputs,
        num_outputs,
        total_bytes: total_bytes_actual,
        rows_per_input,
        payload_cols,
        chunks_per_input,
        map_concurrency,
        read_concurrency,
        shuffle_id,
        shuffle_root,
        seal,
        compression,
    }
}

fn parse_size(s: &str) -> usize {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix("GiB").or_else(|| s.strip_suffix("G")) {
        (n, 1024usize.pow(3))
    } else if let Some(n) = s.strip_suffix("MiB").or_else(|| s.strip_suffix("M")) {
        (n, 1024usize.pow(2))
    } else if let Some(n) = s.strip_suffix("KiB").or_else(|| s.strip_suffix("K")) {
        (n, 1024)
    } else {
        (s, 1)
    };
    num.parse::<usize>().expect("bad size") * mult
}

fn build_schema(payload_cols: usize) -> SchemaRef {
    let mut fields = vec![Field::new("key", DataType::UInt64)];
    for i in 0..payload_cols {
        fields.push(Field::new(format!("v{i}"), DataType::UInt64));
    }
    Arc::new(Schema::new(fields))
}

fn make_input_chunks(
    input_id: u64,
    total_rows: usize,
    payload_cols: usize,
    num_chunks: usize,
    schema: SchemaRef,
) -> Vec<MicroPartition> {
    let num_chunks = num_chunks.max(1);
    let rows_per_chunk = total_rows / num_chunks;
    let mut chunks = Vec::with_capacity(num_chunks);
    for chunk_idx in 0..num_chunks {
        let rows = if chunk_idx == num_chunks - 1 {
            total_rows - rows_per_chunk * (num_chunks - 1)
        } else {
            rows_per_chunk
        };
        let seed = input_id.wrapping_mul(1_000_003).wrapping_add(chunk_idx as u64);
        chunks.push(make_input(seed, rows, payload_cols, schema.clone()));
    }
    chunks
}

fn make_input(input_id: u64, rows: usize, payload_cols: usize, schema: SchemaRef) -> MicroPartition {
    let mut state = input_id.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let key_vals: Vec<u64> = (0..rows).map(|_| next()).collect();
    let mut series: Vec<Series> = Vec::with_capacity(1 + payload_cols);
    series.push(
        UInt64Array::from_field_and_values(Field::new("key", DataType::UInt64), key_vals)
            .into_series(),
    );
    for i in 0..payload_cols {
        let vals: Vec<u64> = (0..rows).map(|_| next()).collect();
        series.push(
            UInt64Array::from_field_and_values(
                Field::new(format!("v{i}"), DataType::UInt64),
                vals,
            )
            .into_series(),
        );
    }
    let batch = RecordBatch::new_unchecked(schema.clone(), series, rows);
    MicroPartition::new_loaded(schema, Arc::new(vec![batch]), None)
}

struct MapTaskResult {
    partition_caches: Vec<PartitionCache>,
    partition_time_ms: f64,
    accumulate_time_ms: f64,
    write_time_ms: f64,
}

/// Accumulate per-output `Vec<MicroPartition>` from chunked input + partition_by_hash.
/// Shared between the writer modes that take `Vec<Vec<MicroPartition>>` (oneshot, multi_file)
/// and the append-mode path which feeds them through the per-partition file writer.
async fn accumulate_per_partition(
    cfg: &BenchConfig,
    input_id: u32,
    schema: &SchemaRef,
) -> DaftResult<(Vec<Vec<MicroPartition>>, f64, f64)> {
    let key_expr = vec![BoundExpr::new_unchecked(bound_col(
        0,
        Field::new("key", DataType::UInt64),
    ))];
    let chunks = make_input_chunks(
        input_id as u64,
        cfg.rows_per_input,
        cfg.payload_cols,
        cfg.chunks_per_input,
        schema.clone(),
    );

    let mut per_partition: Vec<Vec<MicroPartition>> =
        (0..cfg.num_outputs).map(|_| Vec::new()).collect();

    let mut partition_time_ms = 0.0;
    let mut accumulate_time_ms = 0.0;
    for input in chunks {
        let t0 = Instant::now();
        let parts = input.partition_by_hash(&key_expr, cfg.num_outputs)?;
        partition_time_ms += t0.elapsed().as_secs_f64() * 1000.0;

        let t1 = Instant::now();
        for (i, mp) in parts.into_iter().enumerate() {
            per_partition[i].push(mp);
        }
        accumulate_time_ms += t1.elapsed().as_secs_f64() * 1000.0;
    }

    Ok((per_partition, partition_time_ms, accumulate_time_ms))
}

async fn run_map_task_oneshot(
    cfg: &BenchConfig,
    input_id: u32,
    schema: SchemaRef,
    shuffle_dirs: Arc<Vec<String>>,
) -> DaftResult<MapTaskResult> {
    let (per_partition, partition_time_ms, accumulate_time_ms) =
        accumulate_per_partition(cfg, input_id, &schema).await?;

    let t2 = Instant::now();
    let caches = write_partitions_one_shot(
        input_id,
        cfg.shuffle_id,
        &shuffle_dirs,
        schema,
        cfg.compression,
        per_partition,
    )
    .await?;
    let write_time_ms = t2.elapsed().as_secs_f64() * 1000.0;

    Ok(MapTaskResult {
        partition_caches: caches,
        partition_time_ms,
        accumulate_time_ms,
        write_time_ms,
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64) * p).clamp(0.0, sorted.len() as f64 - 1.0) as usize;
    sorted[idx]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> DaftResult<()> {
    if let Ok(level) = std::env::var("DAFT_TRACE") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_new(level)
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_writer(std::io::stderr)
            .try_init();
    }
    let cfg = parse_args();
    let host_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);

    println!("=== seal_bench ===");
    println!("writer:           oneshot");
    println!(
        "seal:             {}",
        match cfg.seal {
            SealMode::Never => "never",
            SealMode::Always => "always",
        }
    );
    println!(
        "compression:      {}",
        match cfg.compression {
            None => "none",
            Some(arrow_ipc::CompressionType::LZ4_FRAME) => "lz4",
            Some(arrow_ipc::CompressionType::ZSTD) => "zstd",
            Some(_) => "other",
        }
    );
    println!("M inputs:         {}", cfg.num_inputs);
    println!("N outputs:        {}", cfg.num_outputs);
    println!("payload cols:     {} u64 (+1 key u64)", cfg.payload_cols);
    println!("total bytes:      {:.2} MiB", cfg.total_bytes as f64 / (1024.0 * 1024.0));
    println!("rows per input:   {}", cfg.rows_per_input);
    println!("chunks/input:     {}", cfg.chunks_per_input);
    println!("map concurrency:  {}", cfg.map_concurrency);
    println!("read concurrency: {}", cfg.read_concurrency);
    println!("host cpus:        {}", host_cpus);
    println!("shuffle root:     {}", cfg.shuffle_root.display());
    println!();

    std::fs::create_dir_all(&cfg.shuffle_root)?;

    let schema = build_schema(cfg.payload_cols);
    let shuffle_dirs = Arc::new(vec![cfg.shuffle_root.to_string_lossy().to_string()]);

    // Start the Flight server up front — append mode needs it during map phase
    // (it owns the shared per-partition writers).
    let server = Arc::new(ShuffleFlightServer::new());
    let server_for_start = server.clone();
    let server_handle = tokio::task::spawn_blocking(move || {
        start_server_loop("127.0.0.1", server_for_start)
    })
    .await
    .expect("server bootstrap panicked");
    let server_address = server_handle.shuffle_address();

    // ----- Phase 1: map (partition + accumulate + write + register-per-task) -----
    //
    // Each map task registers its partition caches with the Flight server as
    // soon as it finishes, mirroring what a Flotilla worker does in production.
    // This is what lets pipelined seal trigger before the map phase is over.
    let phase1 = Instant::now();
    let sem = Arc::new(Semaphore::new(cfg.map_concurrency));
    let mut handles = Vec::with_capacity(cfg.num_inputs);
    for input_id in 0..cfg.num_inputs {
        let cfg_c = cfg.clone();
        let schema_c = schema.clone();
        let dirs_c = shuffle_dirs.clone();
        let server_c = server.clone();
        let sem_c = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem_c.acquire_owned().await.unwrap();
            let result = run_map_task_oneshot(&cfg_c, input_id as u32, schema_c, dirs_c).await?;
            // Per-task register — lets pipelined seal kick off mid-phase.
            server_c
                .register_shuffle_partitions(cfg_c.shuffle_id, result.partition_caches.clone())
                .await?;
            DaftResult::Ok(result)
        }));
    }
    let mut map_results = Vec::with_capacity(cfg.num_inputs);
    for h in handles {
        map_results.push(h.await.expect("map task panicked")?);
    }
    let phase1_ms = phase1.elapsed().as_secs_f64() * 1000.0;

    let mut partition_times: Vec<f64> = map_results.iter().map(|r| r.partition_time_ms).collect();
    let mut accumulate_times: Vec<f64> = map_results.iter().map(|r| r.accumulate_time_ms).collect();
    let mut write_times: Vec<f64> = map_results.iter().map(|r| r.write_time_ms).collect();
    partition_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    accumulate_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    write_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut unique_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
    for r in &map_results {
        for pc in &r.partition_caches {
            for path in &pc.file_paths {
                unique_paths.insert(path.clone());
            }
        }
    }
    let mut all_file_sizes: Vec<u64> = unique_paths
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .collect();
    let total_files = unique_paths.len();
    all_file_sizes.sort_unstable();
    let file_sizes_f: Vec<f64> = all_file_sizes.iter().map(|x| *x as f64).collect();
    let disk_bytes = walk_size(&cfg.shuffle_root);

    println!("--- Phase 1: map ---");
    println!("wall:                       {:.1} ms", phase1_ms);
    println!(
        "  partition_by_hash p50/p99: {:.1} / {:.1} ms",
        percentile(&partition_times, 0.5),
        percentile(&partition_times, 0.99)
    );
    println!(
        "  accumulate per-part p50/p99: {:.1} / {:.1} ms",
        percentile(&accumulate_times, 0.5),
        percentile(&accumulate_times, 0.99)
    );
    println!(
        "  writer (concat+IPC) p50/p99: {:.1} / {:.1} ms",
        percentile(&write_times, 0.5),
        percentile(&write_times, 0.99)
    );
    println!(
        "throughput (in-mem):        {:.1} MiB/s",
        (cfg.total_bytes as f64 / (1024.0 * 1024.0)) / (phase1_ms / 1000.0)
    );
    println!();

    // Write-path attribution. Only meaningful for the oneshot writer; the append
    // writer accumulates its counters in `agg` (CONCAT_US / ENCODE_WRITE_US).
    {
        let w = write_agg_snapshot();
        if w.oneshot_calls > 0 {
            let n = w.spawn_tasks.max(1);
            let nz = w.spawn_tasks_nonempty.max(1);
            println!("--- Write path attribution (oneshot) ---");
            println!("oneshot calls (= map tasks):  {}", w.oneshot_calls);
            println!(
                "spawn tasks (total / nonempty): {} / {}",
                w.spawn_tasks, w.spawn_tasks_nonempty
            );
            println!(
                "join_wall / spawn_total / blocking_wall totals (ms): {:>7.1} / {:>7.1} / {:>7.1}",
                w.join_wall_us as f64 / 1000.0,
                w.spawn_total_us as f64 / 1000.0,
                w.blocking_wall_us as f64 / 1000.0,
            );
            println!(
                "  per-task: spawn_total {:>5} us  (mean per-future service time)",
                w.spawn_total_us / n,
            );
            println!(
                "mp_concat / mp_concat_or_get / try_into totals (ms): {:>7.1} / {:>7.1} / {:>7.1}",
                w.mp_concat_us as f64 / 1000.0,
                w.mp_concat_or_get_us as f64 / 1000.0,
                w.try_into_us as f64 / 1000.0,
            );
            println!(
                "  per nonempty task (us):                              {:>5} / {:>5} / {:>5}",
                w.mp_concat_us / nz,
                w.mp_concat_or_get_us / nz,
                w.try_into_us / nz,
            );
            println!(
                "concat_batches / ipc_write totals (ms):              {:>7.1} / {:>7.1}",
                w.concat_batches_us as f64 / 1000.0,
                w.ipc_write_us as f64 / 1000.0,
            );
            println!(
                "  per nonempty task (us):                              {:>5} / {:>5}",
                w.concat_batches_us / nz,
                w.ipc_write_us / nz,
            );
            println!(
                "file_write_bytes: {:.1} MiB",
                w.file_write_bytes as f64 / (1024.0 * 1024.0)
            );
            {
                use std::sync::atomic::Ordering;
                use daft_shuffles::multi_partition_cache::agg;
                let lt_4k = agg::FLUSH_LT_4K.load(Ordering::Relaxed);
                let lt_16k = agg::FLUSH_LT_16K.load(Ordering::Relaxed);
                let lt_64k = agg::FLUSH_LT_64K.load(Ordering::Relaxed);
                let lt_256k = agg::FLUSH_LT_256K.load(Ordering::Relaxed);
                let lt_1m = agg::FLUSH_LT_1M.load(Ordering::Relaxed);
                let lt_4m = agg::FLUSH_LT_4M.load(Ordering::Relaxed);
                let ge_4m = agg::FLUSH_GE_4M.load(Ordering::Relaxed);
                println!(
                    "flush histogram: <4k={} <16k={} <64k={} <256k={} <1m={} <4m={} ≥4m={}",
                    lt_4k, lt_16k, lt_64k, lt_256k, lt_1m, lt_4m, ge_4m,
                );
            }
            println!();
        }
    }

    println!("--- File stats (pre-seal) ---");
    println!("partition caches total:     {}", cfg.num_inputs * cfg.num_outputs);
    println!("intermediate files total:   {}", total_files);
    println!(
        "file size p50/p90/p99/max:  {:.1} / {:.1} / {:.1} / {:.1} KiB",
        percentile(&file_sizes_f, 0.5) / 1024.0,
        percentile(&file_sizes_f, 0.9) / 1024.0,
        percentile(&file_sizes_f, 0.99) / 1024.0,
        all_file_sizes.last().copied().unwrap_or(0) as f64 / 1024.0,
    );
    println!("on-disk bytes:              {:.2} MiB", disk_bytes as f64 / (1024.0 * 1024.0));
    println!(
        "on-disk / in-memory ratio:  {:.2}x",
        disk_bytes as f64 / cfg.total_bytes as f64
    );
    println!();

    // ----- Phase 2: register (no-op; map tasks already registered) -----
    // Map tasks register their caches as they finish (so pipelined seal can
    // fire mid-phase). This block exists only to keep the phase accounting
    // and CSV column layout stable.
    let phase2 = Instant::now();
    let _ = map_results;
    let phase2_ms = phase2.elapsed().as_secs_f64() * 1000.0;
    println!("--- Phase 2: register ---");
    println!("wall:                       {:.1} ms (registered inline during map)", phase2_ms);
    println!("server addr:                {}", server_address);
    println!();

    // ----- Phase 2.5 (optional): seal -----
    let phase2_5_ms = if cfg.seal == SealMode::Always {
        let t = Instant::now();
        server.seal_shuffle(cfg.shuffle_id).await?;
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let post_disk = walk_size(&cfg.shuffle_root);
        println!("--- Phase 2.5: seal ---");
        println!("wall:                       {:.1} ms", ms);
        println!(
            "post-seal disk bytes:       {:.2} MiB (delta {:+.2} MiB)",
            post_disk as f64 / (1024.0 * 1024.0),
            (post_disk as i64 - disk_bytes as i64) as f64 / (1024.0 * 1024.0)
        );
        println!();
        ms
    } else {
        0.0
    };

    // Optional cold-read: drop page cache + sync to force reads to hit disk.
    // Requires root (process must run under sudo). Used when measuring slow
    // shuffle backends (EBS, S3-fuse) where the page cache would otherwise
    // mask the I/O cost.
    if std::env::var("DAFT_SHUFFLE_BENCH_COLD_READ").is_ok() {
        let _ = std::process::Command::new("sync").status();
        if let Err(e) = std::fs::write("/proc/sys/vm/drop_caches", b"3\n") {
            eprintln!("warning: cold-read drop_caches failed: {} (run under sudo)", e);
        }
    }

    // ----- Phase 3: reduce -----
    let bytes_read = Arc::new(AtomicU64::new(0));
    let batches_read = Arc::new(AtomicUsize::new(0));

    let phase3 = Instant::now();
    let client_manager = FlightClientManager::new();
    let sem = Arc::new(Semaphore::new(cfg.read_concurrency));
    let mut handles = Vec::with_capacity(cfg.num_outputs);
    for partition_idx in 0..cfg.num_outputs {
        let server_address = server_address.clone();
        let schema = schema.clone();
        let num_inputs = cfg.num_inputs;
        let shuffle_id = cfg.shuffle_id;
        let bytes_read = bytes_read.clone();
        let batches_read = batches_read.clone();
        let sem = sem.clone();
        let cm = client_manager.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let ref_ids: Vec<u64> = (0..num_inputs)
                .map(|input_id| partition_ref_id(input_id as u32, partition_idx))
                .collect();
            let mut stream = cm
                .fetch_partition(shuffle_id, &server_address, &ref_ids, schema)
                .await?;
            while let Some(rb) = stream.next().await {
                let rb = rb?;
                bytes_read.fetch_add(rb.size_bytes() as u64, Ordering::Relaxed);
                batches_read.fetch_add(1, Ordering::Relaxed);
            }
            DaftResult::Ok(())
        }));
    }
    for h in handles {
        h.await.expect("read task panicked")?;
    }
    let phase3_ms = phase3.elapsed().as_secs_f64() * 1000.0;
    let bytes_read_total = bytes_read.load(Ordering::Relaxed);
    let batches_read_total = batches_read.load(Ordering::Relaxed);

    println!("--- Phase 3: reduce (Flight read) ---");
    println!("wall:                       {:.1} ms", phase3_ms);
    println!(
        "bytes read:                 {:.2} MiB",
        bytes_read_total as f64 / (1024.0 * 1024.0)
    );
    println!("record batches read:        {}", batches_read_total);
    println!(
        "read throughput:            {:.1} MiB/s",
        (bytes_read_total as f64 / (1024.0 * 1024.0)) / (phase3_ms / 1000.0)
    );
    println!();

    let total_ms = phase1_ms + phase2_ms + phase2_5_ms + phase3_ms;
    println!("--- Summary ---");
    println!("total wall (map+reg+seal+read): {:.1} ms", total_ms);

    // CSV: writer,seal,compression,M,N,bytes,phase1_ms,phase2_ms,seal_ms,phase3_ms,total_ms,files,disk_mib,read_mib
    println!(
        "CSV: {},{},{},{},{},{},{:.1},{:.1},{:.1},{:.1},{:.1},{},{:.1},{:.1}",
        "oneshot",
        match cfg.seal {
            SealMode::Never => "never",
            SealMode::Always => "always",
        },
        match cfg.compression {
            None => "none",
            Some(arrow_ipc::CompressionType::LZ4_FRAME) => "lz4",
            Some(arrow_ipc::CompressionType::ZSTD) => "zstd",
            Some(_) => "other",
        },
        cfg.num_inputs,
        cfg.num_outputs,
        cfg.total_bytes,
        phase1_ms,
        phase2_ms,
        phase2_5_ms,
        phase3_ms,
        total_ms,
        total_files,
        disk_bytes as f64 / (1024.0 * 1024.0),
        bytes_read_total as f64 / (1024.0 * 1024.0)
    );

    log_read_agg_summary("seal_bench end");
    log_write_agg_summary("seal_bench end");

    // Plain-print attribution so we see it without DAFT_TRACE=info.
    let s = read_agg_snapshot();
    let n = s.flightdatas_emitted.max(1);
    let cn = s.client_batches_received.max(1);
    let mb = 1024.0 * 1024.0;
    println!("--- Read path attribution (per FlightData / RecordBatch) ---");
    println!("server flightdatas emitted:   {}", s.flightdatas_emitted);
    println!(
        "server  open / header / body / send-gap totals:  {:>6.1} / {:>6.1} / {:>6.1} / {:>6.1} ms",
        s.open_us as f64 / 1000.0,
        s.msg_header_read_us as f64 / 1000.0,
        s.msg_body_read_us as f64 / 1000.0,
        s.send_gap_us as f64 / 1000.0,
    );
    println!(
        "server  per-msg header / body / send-gap:        {:>6} / {:>6} / {:>6} us",
        s.msg_header_read_us / n,
        s.msg_body_read_us / n,
        s.send_gap_us / n,
    );
    println!("server  bytes shipped: {:.1} MiB", s.bytes_shipped as f64 / mb);
    println!("client  batches received:     {}", s.client_batches_received);
    println!(
        "client  delivery / convert totals:               {:>6.1} / {:>6.1} ms",
        s.client_batch_delivery_us as f64 / 1000.0,
        s.client_convert_us as f64 / 1000.0,
    );
    println!(
        "client  per-batch delivery / convert:            {:>6} / {:>6} us",
        s.client_batch_delivery_us / cn,
        s.client_convert_us / cn,
    );
    if s.concat_opens > 0 || s.concat_out_chunks > 0 {
        let cb = s.concat_source_batches.max(1);
        let oc = s.concat_out_chunks.max(1);
        let opens = s.concat_opens.max(1);
        println!("--- Read-concat path attribution (server-side, summed across reducers) ---");
        println!(
            "opens / source_batches / out_chunks: {} / {} / {}",
            s.concat_opens, s.concat_source_batches, s.concat_out_chunks,
        );
        println!(
            "open / seek / stream_init / decode totals (ms): {:>6.1} / {:>6.1} / {:>6.1} / {:>6.1}",
            s.concat_open_us as f64 / 1000.0,
            s.concat_seek_us as f64 / 1000.0,
            s.concat_stream_init_us as f64 / 1000.0,
            s.concat_decode_us as f64 / 1000.0,
        );
        println!(
            "  per open / per src batch decode (us):         {:>6} / {:>6}",
            s.concat_open_us / opens,
            s.concat_decode_us / cb,
        );
        println!(
            "merge / encode totals (ms):                    {:>6.1} / {:>6.1}",
            s.concat_merge_us as f64 / 1000.0,
            s.concat_encode_us as f64 / 1000.0,
        );
        println!(
            "  per out chunk merge / encode (us):            {:>6} / {:>6}",
            s.concat_merge_us / oc,
            s.concat_encode_us / oc,
        );
    }
    println!();

    drop(server_handle);
    let _ = std::fs::remove_dir_all(&cfg.shuffle_root);

    Ok(())
}

fn walk_size(p: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(p) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += walk_size(&path);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}
