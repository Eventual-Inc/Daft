//! Local benchmark for the existing shuffle pipeline:
//!   gen synthetic MicroPartitions -> partition_by_hash -> InProgressShuffleCache (write)
//!   -> ShuffleFlightServer (register) -> ShuffleFlightClient (read)
//!
//! Reports per-phase wall time, file counts, file size distribution, throughput.
//! Run with: cargo run --release -p daft-shuffles --bin shuffle_bench -- <args>

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
    client::flight_client::ShuffleFlightClient,
    multi_partition_cache::{
        CAP_EVICTIONS, MultiPartitionShuffleCache, log_agg_summary,
    },
    server::flight_server::{ShuffleFlightServer, log_read_agg_summary, start_server_loop},
    shuffle_cache::{InProgressShuffleCache, PartitionCache, partition_ref_id},
};
use futures::StreamExt;
use tokio::sync::Semaphore;

const TARGET_TOTAL_IN_MEMORY_BYTES: usize = 1024 * 1024 * 2000; // matches RepartitionSink

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Current production layout: M*N small files via InProgressShuffleCache.
    Current,
    /// New layout: M files, each with a per-output-partition byte-range index.
    Combined,
}

#[derive(Debug, Clone)]
struct BenchConfig {
    num_inputs: usize,
    num_outputs: usize,
    total_bytes: usize,
    /// rows per generated micropartition (per input)
    rows_per_input: usize,
    /// number of u64 payload columns (in addition to the key column)
    payload_cols: usize,
    chunks_per_input: usize,
    /// concurrent map tasks (writer side)
    map_concurrency: usize,
    /// concurrent reduce tasks (reader side)
    read_concurrency: usize,
    shuffle_id: u64,
    shuffle_root: PathBuf,
    mode: Mode,
}

fn parse_args() -> BenchConfig {
    // CLI: --inputs M --outputs N --bytes B --payload-cols P --map-conc K --read-conc R
    let mut num_inputs: usize = 100;
    let mut num_outputs: usize = 100;
    let mut total_bytes: usize = 1024 * 1024 * 1024; // 1 GiB
    let mut payload_cols: usize = 4;
    let mut map_concurrency: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut read_concurrency: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut mode = Mode::Current;
    // Split each input into this many MicroPartitions before partitioning, mirroring how
    // BlockingSink::sink in production is called many times per map task.
    let mut chunks_per_input: usize = 8;

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
            "--mode" => {
                mode = match val.as_str() {
                    "current" => Mode::Current,
                    "combined" => Mode::Combined,
                    other => panic!("unknown mode: {other}"),
                }
            }
            "--chunks-per-input" => chunks_per_input = val.parse().unwrap(),
            _ => panic!("unknown arg: {arg}"),
        }
    }

    // Each row is (1 key + payload_cols payload) u64s = 8 * (1 + payload_cols) bytes
    let row_bytes = 8 * (1 + payload_cols);
    let total_rows = total_bytes / row_bytes;
    let rows_per_input = total_rows / num_inputs;
    let total_bytes_actual = rows_per_input * num_inputs * row_bytes;

    let shuffle_id = 1u64;
    let shuffle_root = std::env::temp_dir().join(format!(
        "shuffle_bench_{}_{}_{}",
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
        mode,
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
        // Vary seed per chunk so keys don't repeat across chunks.
        let seed = input_id.wrapping_mul(1_000_003).wrapping_add(chunk_idx as u64);
        chunks.push(make_input(seed, rows, payload_cols, schema.clone()));
    }
    chunks
}

fn make_input(input_id: u64, rows: usize, payload_cols: usize, schema: SchemaRef) -> MicroPartition {
    // Key column: pseudo-random by input_id so hash partitioning spreads across buckets.
    // Use a cheap mix; we want unique-ish keys without an extra dep.
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
    _input_id: u32,
    partition_caches: Vec<PartitionCache>,
    partition_time_ms: f64,
    push_time_ms: f64,
    close_time_ms: f64,
}

async fn run_map_task(
    cfg: &BenchConfig,
    input_id: u32,
    schema: SchemaRef,
    shuffle_dirs: Arc<Vec<String>>,
    target_filesize: usize,
) -> DaftResult<MapTaskResult> {
    let key_expr = vec![BoundExpr::new_unchecked(bound_col(
        0,
        Field::new("key", DataType::UInt64),
    ))];

    // Build N caches for this map task (mirrors RepartitionSink::make_state Flight branch).
    let mut caches = Vec::with_capacity(cfg.num_outputs);
    for partition_idx in 0..cfg.num_outputs {
        caches.push(Arc::new(InProgressShuffleCache::try_new(
            partition_ref_id(input_id, partition_idx),
            schema.clone(),
            &shuffle_dirs,
            cfg.shuffle_id,
            target_filesize,
            None,
        )?));
    }

    // Split the input into K chunks. Mirrors how RepartitionSink::sink in production is
    // called many times per map task as upstream MicroPartitions arrive.
    let chunks = make_input_chunks(
        input_id as u64,
        cfg.rows_per_input,
        cfg.payload_cols,
        cfg.chunks_per_input,
        schema.clone(),
    );

    let mut partition_time_ms = 0.0;
    let mut push_time_ms = 0.0;
    for input in chunks {
        let t0 = Instant::now();
        let parts = input.partition_by_hash(&key_expr, cfg.num_outputs)?;
        partition_time_ms += t0.elapsed().as_secs_f64() * 1000.0;

        let t1 = Instant::now();
        let pushes = caches
            .iter()
            .zip(parts.into_iter())
            .map(|(cache, mp)| {
                let cache = cache.clone();
                async move { cache.push_partition_data(mp).await }
            });
        futures::future::try_join_all(pushes).await?;
        push_time_ms += t1.elapsed().as_secs_f64() * 1000.0;
    }

    // Close all caches.
    let t2 = Instant::now();
    let closes = caches.iter().map(|c| {
        let c = c.clone();
        async move { c.close().await }
    });
    let partition_caches = futures::future::try_join_all(closes).await?;
    let close_time_ms = t2.elapsed().as_secs_f64() * 1000.0;

    Ok(MapTaskResult {
        _input_id: input_id,
        partition_caches,
        partition_time_ms,
        push_time_ms,
        close_time_ms,
    })
}

async fn run_map_task_combined(
    cfg: &BenchConfig,
    input_id: u32,
    schema: SchemaRef,
    shuffle_dirs: Arc<Vec<String>>,
) -> DaftResult<MapTaskResult> {
    let key_expr = vec![BoundExpr::new_unchecked(bound_col(
        0,
        Field::new("key", DataType::UInt64),
    ))];

    // One cache per map task (mirroring proposal 1: single IPC file with partition-range footer).
    let cache = Arc::new(MultiPartitionShuffleCache::try_new(
        input_id,
        cfg.num_outputs,
        schema.clone(),
        &shuffle_dirs,
        cfg.shuffle_id,
    )?);

    let chunks = make_input_chunks(
        input_id as u64,
        cfg.rows_per_input,
        cfg.payload_cols,
        cfg.chunks_per_input,
        schema.clone(),
    );

    let mut partition_time_ms = 0.0;
    let mut push_time_ms = 0.0;
    for input in chunks {
        let t0 = Instant::now();
        let parts = input.partition_by_hash(&key_expr, cfg.num_outputs)?;
        partition_time_ms += t0.elapsed().as_secs_f64() * 1000.0;

        let t1 = Instant::now();
        cache.push_all(parts).await?;
        push_time_ms += t1.elapsed().as_secs_f64() * 1000.0;
    }

    let t2 = Instant::now();
    let partition_caches = cache.close().await?;
    let close_time_ms = t2.elapsed().as_secs_f64() * 1000.0;

    Ok(MapTaskResult {
        _input_id: input_id,
        partition_caches,
        partition_time_ms,
        push_time_ms,
        close_time_ms,
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
    // Install a tracing subscriber if DAFT_TRACE is set (e.g. DAFT_TRACE=info).
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

    println!("=== shuffle_bench ===");
    println!(
        "mode:            {}",
        match cfg.mode {
            Mode::Current => "current (M*N small files)",
            Mode::Combined => "combined (M files with range index)",
        }
    );
    println!("M inputs:        {}", cfg.num_inputs);
    println!("N outputs:       {}", cfg.num_outputs);
    println!("payload cols:    {} u64 (+ 1 key u64)", cfg.payload_cols);
    println!("total bytes:     {:.2} MiB", cfg.total_bytes as f64 / (1024.0 * 1024.0));
    println!("rows per input:  {}", cfg.rows_per_input);
    println!("chunks/input:    {}", cfg.chunks_per_input);
    println!("map concurrency: {}", cfg.map_concurrency);
    println!("read concurrency:{}", cfg.read_concurrency);
    println!("host cpus:       {}", host_cpus);
    println!("shuffle root:    {}", cfg.shuffle_root.display());
    println!();

    std::fs::create_dir_all(&cfg.shuffle_root)?;

    let target_filesize = (TARGET_TOTAL_IN_MEMORY_BYTES / cfg.num_outputs)
        .clamp(1024 * 1024 * 8, 1024 * 1024 * 128);
    println!("target_filesize per partition: {} bytes", target_filesize);

    let schema = build_schema(cfg.payload_cols);
    let shuffle_dirs = Arc::new(vec![cfg.shuffle_root.to_string_lossy().to_string()]);

    // ----- Phase 1: map (partition + write) -----
    let phase1 = Instant::now();
    let sem = Arc::new(Semaphore::new(cfg.map_concurrency));
    let mut handles = Vec::with_capacity(cfg.num_inputs);
    for input_id in 0..cfg.num_inputs {
        let cfg_c = cfg.clone();
        let schema_c = schema.clone();
        let dirs_c = shuffle_dirs.clone();
        let sem_c = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem_c.acquire_owned().await.unwrap();
            match cfg_c.mode {
                Mode::Current => {
                    run_map_task(&cfg_c, input_id as u32, schema_c, dirs_c, target_filesize).await
                }
                Mode::Combined => {
                    run_map_task_combined(&cfg_c, input_id as u32, schema_c, dirs_c).await
                }
            }
        }));
    }
    let mut map_results = Vec::with_capacity(cfg.num_inputs);
    for h in handles {
        map_results.push(h.await.expect("map task panicked")?);
    }
    let phase1_ms = phase1.elapsed().as_secs_f64() * 1000.0;

    // Aggregate map metrics.
    let mut partition_times: Vec<f64> = map_results.iter().map(|r| r.partition_time_ms).collect();
    let mut push_times: Vec<f64> = map_results.iter().map(|r| r.push_time_ms).collect();
    let mut close_times: Vec<f64> = map_results.iter().map(|r| r.close_time_ms).collect();
    partition_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    push_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    close_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // File stats. In combined mode the same physical file is referenced by N partition caches,
    // so de-dup by file path before reporting count and size distribution.
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

    // Disk usage (walk the shuffle root).
    let disk_bytes = walk_size(&cfg.shuffle_root);

    println!("--- Phase 1: map (partition + write + close) ---");
    println!("wall:                         {:.1} ms", phase1_ms);
    if cfg.mode == Mode::Combined {
        let evictions = CAP_EVICTIONS.load(std::sync::atomic::Ordering::Relaxed);
        println!("memory-cap evictions:         {}", evictions);
    }
    println!(
        "  partition_by_hash p50/p99:  {:.1} / {:.1} ms",
        percentile(&partition_times, 0.5),
        percentile(&partition_times, 0.99)
    );
    println!(
        "  push (all parts) p50/p99:   {:.1} / {:.1} ms",
        percentile(&push_times, 0.5),
        percentile(&push_times, 0.99)
    );
    println!(
        "  close (all caches) p50/p99: {:.1} / {:.1} ms",
        percentile(&close_times, 0.5),
        percentile(&close_times, 0.99)
    );
    println!(
        "throughput (in-memory bytes): {:.1} MiB/s",
        (cfg.total_bytes as f64 / (1024.0 * 1024.0)) / (phase1_ms / 1000.0)
    );
    println!();

    println!("--- File stats ---");
    println!("partition caches total:       {}", cfg.num_inputs * cfg.num_outputs);
    println!("intermediate files total:     {}", total_files);
    println!(
        "  files / partition cache avg: {:.2}",
        total_files as f64 / (cfg.num_inputs * cfg.num_outputs) as f64
    );
    println!(
        "file size p50/p90/p99/max:    {:.1} / {:.1} / {:.1} / {:.1} KiB",
        percentile(&file_sizes_f, 0.5) / 1024.0,
        percentile(&file_sizes_f, 0.9) / 1024.0,
        percentile(&file_sizes_f, 0.99) / 1024.0,
        all_file_sizes.last().copied().unwrap_or(0) as f64 / 1024.0,
    );
    println!(
        "on-disk bytes:                {:.2} MiB",
        disk_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "on-disk / in-memory ratio:    {:.2}x",
        disk_bytes as f64 / cfg.total_bytes as f64
    );
    println!();

    // ----- Phase 2: register with flight server -----
    let phase2 = Instant::now();
    let server = Arc::new(ShuffleFlightServer::new());
    // start_server_loop uses blocking_recv internally, so it must be on a blocking thread.
    let server_for_start = server.clone();
    let server_handle = tokio::task::spawn_blocking(move || {
        start_server_loop("127.0.0.1", server_for_start)
    })
    .await
    .expect("server bootstrap panicked");
    let mut all_caches: Vec<PartitionCache> =
        Vec::with_capacity(cfg.num_inputs * cfg.num_outputs);
    for r in map_results {
        all_caches.extend(r.partition_caches);
    }
    server
        .register_shuffle_partitions(cfg.shuffle_id, all_caches)
        .await?;
    let phase2_ms = phase2.elapsed().as_secs_f64() * 1000.0;
    let server_address = server_handle.shuffle_address();
    println!("--- Phase 2: server start + register ---");
    println!("wall:                         {:.1} ms", phase2_ms);
    println!("server addr:                  {}", server_address);
    println!();

    // ----- Phase 3: reduce (read via flight client) -----
    // One read task per output partition: each reads M files (one slice per input map task)
    // by passing all M (input_id, partition_idx) ref ids in a single ticket.
    let bytes_read = Arc::new(AtomicU64::new(0));
    let batches_read = Arc::new(AtomicUsize::new(0));

    let phase3 = Instant::now();
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
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let mut client = ShuffleFlightClient::new(server_address);
            let ref_ids: Vec<u64> = (0..num_inputs)
                .map(|input_id| partition_ref_id(input_id as u32, partition_idx))
                .collect();
            let mut stream = client
                .get_partition(shuffle_id, &ref_ids, schema)
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

    println!("--- Phase 3: reduce (flight read) ---");
    println!("wall:                         {:.1} ms", phase3_ms);
    println!(
        "bytes read:                   {:.2} MiB",
        bytes_read_total as f64 / (1024.0 * 1024.0)
    );
    println!("record batches read:          {}", batches_read_total);
    println!(
        "read throughput:              {:.1} MiB/s",
        (bytes_read_total as f64 / (1024.0 * 1024.0)) / (phase3_ms / 1000.0)
    );
    if batches_read_total > 0 {
        println!(
            "avg bytes per batch read:     {:.1} KiB",
            bytes_read_total as f64 / batches_read_total as f64 / 1024.0
        );
    }
    println!();

    println!("--- Summary ---");
    println!(
        "total wall (map+register+read): {:.1} ms",
        phase1_ms + phase2_ms + phase3_ms
    );
    println!(
        "CSV: {},{},{:.1},{:.1},{:.1},{},{:.1},{:.1}",
        cfg.num_inputs,
        cfg.num_outputs,
        phase1_ms,
        phase2_ms,
        phase3_ms,
        total_files,
        percentile(&file_sizes_f, 0.5) / 1024.0,
        bytes_read_total as f64 / (1024.0 * 1024.0)
    );

    // Emit aggregate summaries (subscriber-respecting; no-op if DAFT_TRACE unset).
    log_agg_summary("bench end");
    log_read_agg_summary("bench end");

    // Cleanup: drop the server handle, remove the temp dir.
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
