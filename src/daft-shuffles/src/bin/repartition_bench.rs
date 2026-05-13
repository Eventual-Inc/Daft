//! Local benchmark for the hot path of `RepartitionSink::sink`:
//!   - W parallel workers stream M morsels each
//!   - each morsel is hash-partitioned into N buckets
//!   - worker state is `Vec<Vec<MicroPartition>>` (one Vec per output partition)
//!   - finalize flattens worker states per-partition and concats
//!
//! Sweeps morsel size (S/M/L), partition count (S/M/L), and compares strategies:
//!   - baseline       : current sink behaviour (partition_by_hash per morsel)
//!   - defer          : store (morsel, partition_idx_per_row); single take at finalize
//!   - accumulate     : buffer morsels per worker until a size threshold, then partition
//!   - hash_only      : hash per morsel; concat + partition once at finalize (no rem-per-morsel)
//!
//! Run with: cargo run --release -p daft-shuffles --bin repartition_bench -- <args>

use std::{
    ops::Rem,
    sync::Arc,
    time::{Duration, Instant},
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

// ---------- config ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MorselSize {
    Small,  // ~256 KiB
    Medium, // ~1 MiB
    Large,  // ~8 MiB
}

impl MorselSize {
    fn rows(self, payload_cols: usize) -> usize {
        let bytes = match self {
            Self::Small => 256 * 1024,
            Self::Medium => 1024 * 1024,
            Self::Large => 8 * 1024 * 1024,
        };
        bytes / (8 * (1 + payload_cols))
    }
    fn name(self) -> &'static str {
        match self {
            Self::Small => "S",
            Self::Medium => "M",
            Self::Large => "L",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartCount {
    Small,  // 8
    Medium, // 64
    Large,  // 256
}

impl PartCount {
    fn count(self) -> usize {
        match self {
            Self::Small => 8,
            Self::Medium => 64,
            Self::Large => 256,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Small => "S",
            Self::Medium => "M",
            Self::Large => "L",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Strategy {
    Baseline,
    PerMorselRadix, // radix kernel, no buffering — isolates the kernel speedup
    Defer,
    Accumulate(usize), // accumulate threshold in bytes
    AccumulateScatter(usize),
    AccumulateRadix(usize),
    HashOnly,
}

impl Strategy {
    fn name(self) -> String {
        match self {
            Self::Baseline => "baseline".into(),
            Self::PerMorselRadix => "permorsel-radix".into(),
            Self::Defer => "defer".into(),
            Self::Accumulate(t) => format!("accum-{}M", t / (1024 * 1024)),
            Self::AccumulateScatter(t) => format!("scat-{}M", t / (1024 * 1024)),
            Self::AccumulateRadix(t) => format!("radix-{}M", t / (1024 * 1024)),
            Self::HashOnly => "hashonly".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushKind {
    Take,
    Scatter,
    Radix,
}

#[derive(Debug, Clone)]
struct BenchConfig {
    num_workers: usize,
    morsels_per_worker: usize,
    payload_cols: usize,
    accum_threshold_bytes: usize,
}

// ---------- data gen ----------

fn make_morsel(seed: u64, rows: usize, payload_cols: usize, schema: SchemaRef) -> MicroPartition {
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let key: Vec<u64> = (0..rows).map(|_| next()).collect();
    let mut series: Vec<Series> = Vec::with_capacity(1 + payload_cols);
    series.push(
        UInt64Array::from_field_and_values(Field::new("key", DataType::UInt64), key).into_series(),
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

fn gen_morsels(
    cfg: &BenchConfig,
    morsel_size: MorselSize,
    schema: SchemaRef,
) -> Vec<Vec<MicroPartition>> {
    let rows = morsel_size.rows(cfg.payload_cols);
    let mut per_worker = Vec::with_capacity(cfg.num_workers);
    for w in 0..cfg.num_workers {
        let mut morsels = Vec::with_capacity(cfg.morsels_per_worker);
        for m in 0..cfg.morsels_per_worker {
            let seed = (w as u64) * 1_000_003 + m as u64;
            morsels.push(make_morsel(seed, rows, cfg.payload_cols, schema.clone()));
        }
        per_worker.push(morsels);
    }
    per_worker
}

// ---------- phase timing ----------

#[derive(Default, Clone, Copy)]
struct Phase {
    total: Duration,
    count: u64,
}

impl Phase {
    fn add(&mut self, d: Duration) {
        self.total += d;
        self.count += 1;
    }
}

impl std::ops::Add for Phase {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            total: self.total + rhs.total,
            count: self.count + rhs.count,
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Timings {
    hash: Phase,
    rem: Phase,
    take_loop: Phase,
    push: Phase,
    concat_morsels: Phase,
    accum_concat: Phase,
    finalize_take: Phase,
    finalize_concat: Phase,
    sink_total: Phase,
    wall: Duration,
}

impl std::ops::Add for Timings {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            hash: self.hash + rhs.hash,
            rem: self.rem + rhs.rem,
            take_loop: self.take_loop + rhs.take_loop,
            push: self.push + rhs.push,
            concat_morsels: self.concat_morsels + rhs.concat_morsels,
            accum_concat: self.accum_concat + rhs.accum_concat,
            finalize_take: self.finalize_take + rhs.finalize_take,
            finalize_concat: self.finalize_concat + rhs.finalize_concat,
            sink_total: self.sink_total + rhs.sink_total,
            wall: self.wall + rhs.wall,
        }
    }
}

// ---------- strategy: baseline ----------
// Mirrors the current RepartitionSink::sink path.

fn run_baseline(
    morsels: Vec<MicroPartition>,
    num_partitions: usize,
    key_expr: &[BoundExpr],
) -> DaftResult<(Vec<Vec<MicroPartition>>, Timings)> {
    run_per_morsel(morsels, num_partitions, key_expr, FlushKind::Take)
}

fn run_per_morsel_radix(
    morsels: Vec<MicroPartition>,
    num_partitions: usize,
    key_expr: &[BoundExpr],
) -> DaftResult<(Vec<Vec<MicroPartition>>, Timings)> {
    run_per_morsel(morsels, num_partitions, key_expr, FlushKind::Radix)
}

fn run_per_morsel(
    morsels: Vec<MicroPartition>,
    num_partitions: usize,
    key_expr: &[BoundExpr],
    kind: FlushKind,
) -> DaftResult<(Vec<Vec<MicroPartition>>, Timings)> {
    let mut t = Timings::default();
    let mut per_partition: Vec<Vec<MicroPartition>> =
        (0..num_partitions).map(|_| Vec::new()).collect();

    for input in morsels {
        let t_sink = Instant::now();

        let tables = input.record_batches();
        let parts: Vec<Vec<RecordBatch>> = tables
            .iter()
            .map(|tbl| {
                let key_tbl = tbl.eval_expression_list(key_expr)?;
                let t_hash = Instant::now();
                let hashes = key_tbl.hash_rows()?;
                t.hash.add(t_hash.elapsed());

                let t_rem = Instant::now();
                let targets = hashes.rem(&UInt64Array::from_slice(
                    "num_partitions",
                    &[num_partitions as u64],
                ))?;
                t.rem.add(t_rem.elapsed());

                let t_take = Instant::now();
                let out = match kind {
                    FlushKind::Take => partition_by_index(tbl, &targets, num_partitions)?,
                    FlushKind::Scatter => scatter_u64_batch(tbl, &targets, num_partitions)?,
                    FlushKind::Radix => radix_permute(tbl, &targets, num_partitions)?,
                };
                t.take_loop.add(t_take.elapsed());
                Ok::<_, common_error::DaftError>(out)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let parts = transpose_to_mps(parts, input.schema(), num_partitions);

        let t_push = Instant::now();
        for (acc, p) in per_partition.iter_mut().zip(parts) {
            acc.push(p);
        }
        t.push.add(t_push.elapsed());

        t.sink_total.add(t_sink.elapsed());
    }

    Ok((per_partition, t))
}

// ---------- strategy: defer (single take per partition at finalize) ----------

fn run_defer(
    morsels: Vec<MicroPartition>,
    num_partitions: usize,
    key_expr: &[BoundExpr],
) -> DaftResult<(Vec<Vec<MicroPartition>>, Timings)> {
    let mut t = Timings::default();
    // Per morsel: keep the morsel and a UInt64Array of partition indices per row.
    let mut staged: Vec<(MicroPartition, UInt64Array)> = Vec::with_capacity(morsels.len());

    for input in morsels {
        let t_sink = Instant::now();
        let tables = input.record_batches();
        // Defer to a single-chunk fastpath: most morsels have one RecordBatch.
        // For multi-chunk, we hash each chunk separately and store side-by-side.
        // To keep the API simple, concat to a single RecordBatch up front.
        let combined = if tables.len() == 1 {
            tables[0].clone()
        } else {
            let t_c = Instant::now();
            let c = RecordBatch::concat(tables)?;
            t.concat_morsels.add(t_c.elapsed());
            c
        };

        let key_tbl = combined.eval_expression_list(key_expr)?;
        let t_hash = Instant::now();
        let hashes = key_tbl.hash_rows()?;
        t.hash.add(t_hash.elapsed());

        let t_rem = Instant::now();
        let targets = hashes.rem(&UInt64Array::from_slice(
            "num_partitions",
            &[num_partitions as u64],
        ))?;
        t.rem.add(t_rem.elapsed());

        // No take here. Stash and proceed.
        let mp =
            MicroPartition::new_loaded(input.schema(), Arc::new(vec![combined]), None);
        staged.push((mp, targets));
        t.sink_total.add(t_sink.elapsed());
    }

    // Finalize: per worker, build one big RecordBatch and one big targets array, then take N times.
    let schema = if let Some((mp, _)) = staged.first() {
        mp.schema()
    } else {
        return Ok((
            (0..num_partitions).map(|_| Vec::new()).collect(),
            t,
        ));
    };

    let t_ac = Instant::now();
    let mut batches: Vec<RecordBatch> = Vec::with_capacity(staged.len());
    let mut target_chunks: Vec<UInt64Array> = Vec::with_capacity(staged.len());
    for (mp, targets) in staged {
        let tables = mp.record_batches();
        batches.extend(tables.iter().cloned());
        target_chunks.push(targets);
    }
    let big_batch = RecordBatch::concat(&batches)?;
    let big_targets = concat_u64_arrays(target_chunks);
    t.accum_concat.add(t_ac.elapsed());

    let t_take = Instant::now();
    let part_batches = partition_by_index(&big_batch, &big_targets, num_partitions)?;
    t.finalize_take.add(t_take.elapsed());

    let per_partition: Vec<Vec<MicroPartition>> = part_batches
        .into_iter()
        .map(|b| {
            vec![MicroPartition::new_loaded(
                schema.clone(),
                Arc::new(vec![b]),
                None,
            )]
        })
        .collect();
    Ok((per_partition, t))
}

// ---------- strategy: accumulate (size-threshold buffering then partition) ----------

fn run_accumulate(
    morsels: Vec<MicroPartition>,
    num_partitions: usize,
    key_expr: &[BoundExpr],
    threshold_bytes: usize,
    kind: FlushKind,
) -> DaftResult<(Vec<Vec<MicroPartition>>, Timings)> {
    let mut t = Timings::default();
    let mut per_partition: Vec<Vec<MicroPartition>> =
        (0..num_partitions).map(|_| Vec::new()).collect();
    let mut buffered: Vec<MicroPartition> = Vec::new();
    let mut buffered_bytes: usize = 0;

    let flush =
        |buffered: &mut Vec<MicroPartition>,
         t: &mut Timings,
         per_partition: &mut Vec<Vec<MicroPartition>>|
         -> DaftResult<()> {
            if buffered.is_empty() {
                return Ok(());
            }
            let t_sink = Instant::now();

            // Concat buffered morsels into one RecordBatch.
            let schema = buffered[0].schema();
            let mut batches = Vec::new();
            for mp in buffered.drain(..) {
                batches.extend(mp.record_batches().iter().cloned());
            }
            let t_c = Instant::now();
            let big = RecordBatch::concat(&batches)?;
            t.concat_morsels.add(t_c.elapsed());

            let key_tbl = big.eval_expression_list(key_expr)?;
            let t_hash = Instant::now();
            let hashes = key_tbl.hash_rows()?;
            t.hash.add(t_hash.elapsed());

            let t_rem = Instant::now();
            let targets = hashes.rem(&UInt64Array::from_slice(
                "num_partitions",
                &[num_partitions as u64],
            ))?;
            t.rem.add(t_rem.elapsed());

            let t_take = Instant::now();
            let parts = match kind {
                FlushKind::Take => partition_by_index(&big, &targets, num_partitions)?,
                FlushKind::Scatter => scatter_u64_batch(&big, &targets, num_partitions)?,
                FlushKind::Radix => radix_permute(&big, &targets, num_partitions)?,
            };
            t.take_loop.add(t_take.elapsed());

            let t_push = Instant::now();
            for (acc, p) in per_partition.iter_mut().zip(parts) {
                acc.push(MicroPartition::new_loaded(
                    schema.clone(),
                    Arc::new(vec![p]),
                    None,
                ));
            }
            t.push.add(t_push.elapsed());

            t.sink_total.add(t_sink.elapsed());
            Ok(())
        };

    for input in morsels {
        let bytes = input.size_bytes() as usize;
        buffered_bytes += bytes;
        buffered.push(input);
        if buffered_bytes >= threshold_bytes {
            flush(&mut buffered, &mut t, &mut per_partition)?;
            buffered_bytes = 0;
        }
    }
    flush(&mut buffered, &mut t, &mut per_partition)?;
    Ok((per_partition, t))
}

// ---------- strategy: hash_only (hash per morsel, partition once at finalize) ----------
// Like `defer`, but the per-morsel work is reduced to just hashing — no rem, no allocation
// of a per-morsel UInt64Array of partition indices. We compute everything at finalize.

fn run_hash_only(
    morsels: Vec<MicroPartition>,
    num_partitions: usize,
    key_expr: &[BoundExpr],
) -> DaftResult<(Vec<Vec<MicroPartition>>, Timings)> {
    let mut t = Timings::default();
    let mut staged_batches: Vec<RecordBatch> = Vec::with_capacity(morsels.len());
    let mut staged_hashes: Vec<UInt64Array> = Vec::with_capacity(morsels.len());
    let mut schema = None;

    for input in morsels {
        let t_sink = Instant::now();
        let tables = input.record_batches();
        let combined = if tables.len() == 1 {
            tables[0].clone()
        } else {
            let t_c = Instant::now();
            let c = RecordBatch::concat(tables)?;
            t.concat_morsels.add(t_c.elapsed());
            c
        };
        let key_tbl = combined.eval_expression_list(key_expr)?;
        let t_hash = Instant::now();
        let hashes = key_tbl.hash_rows()?;
        t.hash.add(t_hash.elapsed());

        if schema.is_none() {
            schema = Some(input.schema());
        }
        staged_batches.push(combined);
        staged_hashes.push(hashes);
        t.sink_total.add(t_sink.elapsed());
    }

    let schema = match schema {
        Some(s) => s,
        None => {
            return Ok((
                (0..num_partitions).map(|_| Vec::new()).collect(),
                t,
            ));
        }
    };

    let t_ac = Instant::now();
    let big_batch = RecordBatch::concat(&staged_batches)?;
    let big_hashes = concat_u64_arrays(staged_hashes);
    t.accum_concat.add(t_ac.elapsed());

    let t_rem = Instant::now();
    let targets = big_hashes.rem(&UInt64Array::from_slice(
        "num_partitions",
        &[num_partitions as u64],
    ))?;
    t.rem.add(t_rem.elapsed());

    let t_take = Instant::now();
    let parts = partition_by_index(&big_batch, &targets, num_partitions)?;
    t.finalize_take.add(t_take.elapsed());

    let per_partition: Vec<Vec<MicroPartition>> = parts
        .into_iter()
        .map(|b| {
            vec![MicroPartition::new_loaded(
                schema.clone(),
                Arc::new(vec![b]),
                None,
            )]
        })
        .collect();
    Ok((per_partition, t))
}

// ---------- shared helpers ----------

/// Reproduction of `RecordBatch::partition_by_index` (which is private to the crate).
fn partition_by_index(
    batch: &RecordBatch,
    targets: &UInt64Array,
    num_partitions: usize,
) -> DaftResult<Vec<RecordBatch>> {
    let mut output_to_input_idx: Vec<Vec<u64>> =
        vec![Vec::with_capacity(batch.len() / num_partitions.max(1)); num_partitions];
    for (s_idx, t_idx) in targets.values().iter().enumerate() {
        output_to_input_idx[*t_idx as usize].push(s_idx as u64);
    }
    output_to_input_idx
        .into_iter()
        .map(|v| {
            let idx = UInt64Array::from_vec("idx", v);
            batch.take(&idx)
        })
        .collect()
}

/// Single-pass column-wise scatter for an all-u64 RecordBatch.
/// Avoids N×ncols allocations + N take-dispatches by:
///   1) histogramming partition counts (one pass over targets)
///   2) pre-allocating per-partition output buffers (count + cumulative write head)
///   3) one row-major pass that writes each (col, row) value to its output position
///
/// Compared with `partition_by_index`:
///   * fewer allocations: N×ncols+N×Vec<u64> → N×ncols (no index vec)
///   * single value-copy pass instead of two (bucketize + take)
///   * tight inline loop; no take dispatch
fn scatter_u64_batch(
    batch: &RecordBatch,
    targets: &UInt64Array,
    num_partitions: usize,
) -> DaftResult<Vec<RecordBatch>> {
    let nrows = batch.len();
    let ncols = batch.num_columns();
    let target_vals = targets.values();
    debug_assert_eq!(target_vals.len(), nrows);

    // Histogram per partition.
    let mut counts = vec![0usize; num_partitions];
    for t in target_vals.iter() {
        counts[*t as usize] += 1;
    }

    // Pre-extract input column buffers, then borrow slices into them.
    let input_bufs: Vec<_> = (0..ncols)
        .map(|c| batch.get_column(c).u64().map(|a| a.values()))
        .collect::<DaftResult<Vec<_>>>()?;
    let input_cols: Vec<&[u64]> = input_bufs.iter().map(|b| b.as_ref()).collect();

    // Allocate per-partition output buffers, one per column, sized exactly.
    // Use raw Vec<u64> for scatter — we can wrap as UInt64Array at the end.
    let mut out_cols: Vec<Vec<Vec<u64>>> = (0..num_partitions)
        .map(|p| {
            (0..ncols)
                .map(|_| Vec::with_capacity(counts[p]))
                .collect::<Vec<_>>()
        })
        .collect();

    // Single row-major scatter. We touch each (col, row) once.
    // Order is `for r in rows: for c in cols: write` so we hit cache for inputs,
    // and outputs are touched in N×cols pattern — likely L2-resident for moderate N.
    for r in 0..nrows {
        let p = target_vals[r] as usize;
        let pcols = unsafe { out_cols.get_unchecked_mut(p) };
        for c in 0..ncols {
            unsafe {
                let v = *input_cols.get_unchecked(c).get_unchecked(r);
                pcols.get_unchecked_mut(c).push(v);
            }
        }
    }

    // Build RecordBatches from the per-partition column vecs.
    let schema = batch.schema.clone();
    let fields: Vec<Field> = schema.fields().iter().cloned().collect();
    let mut out_batches = Vec::with_capacity(num_partitions);
    for (p, cols) in out_cols.into_iter().enumerate() {
        let n = counts[p];
        let series: Vec<Series> = cols
            .into_iter()
            .zip(fields.iter())
            .map(|(buf, field)| {
                UInt64Array::from_field_and_values(field.clone(), buf).into_series()
            })
            .collect();
        out_batches.push(RecordBatch::new_unchecked(schema.clone(), series, n));
    }
    Ok(out_batches)
}

/// Type-generic single-pass partition via radix-permutation + zero-copy slice.
///   1) histogram counts per partition
///   2) prefix sum → output start offset per partition
///   3) permutation pass: for each row, write row index to perm[offsets[p]++]
///      → after this, perm groups rows by partition contiguously
///   4) ONE take call on the full permutation (single alloc per column)
///   5) slice the result into N RecordBatches (zero-copy on columns)
fn radix_permute(
    batch: &RecordBatch,
    targets: &UInt64Array,
    num_partitions: usize,
) -> DaftResult<Vec<RecordBatch>> {
    let nrows = batch.len();
    let target_vals = targets.values();
    debug_assert_eq!(target_vals.len(), nrows);

    // Step 1: histogram.
    let mut counts = vec![0usize; num_partitions];
    for t in target_vals.iter() {
        counts[*t as usize] += 1;
    }

    // Step 2: prefix sum into offsets (also retain a copy for slice boundaries).
    let mut offsets = vec![0usize; num_partitions];
    let mut acc = 0usize;
    for p in 0..num_partitions {
        offsets[p] = acc;
        acc += counts[p];
    }
    let starts: Vec<usize> = offsets.clone();

    // Step 3: build permutation.
    let mut perm: Vec<u64> = vec![0u64; nrows];
    for (r, t) in target_vals.iter().enumerate() {
        let p = *t as usize;
        let off = unsafe { offsets.get_unchecked_mut(p) };
        unsafe {
            *perm.get_unchecked_mut(*off) = r as u64;
        }
        *off += 1;
    }

    // Step 4: single take.
    let perm_arr = UInt64Array::from_vec("perm", perm);
    let taken = batch.take(&perm_arr)?;

    // Step 5: slice into N pieces.
    let mut out = Vec::with_capacity(num_partitions);
    for p in 0..num_partitions {
        let start = starts[p];
        let end = start + counts[p];
        out.push(taken.slice(start, end)?);
    }
    Ok(out)
}

fn concat_u64_arrays(parts: Vec<UInt64Array>) -> UInt64Array {
    let mut out: Vec<u64> = Vec::new();
    for p in parts {
        out.extend(p.values().iter().copied());
    }
    UInt64Array::from_vec("concat", out)
}

fn transpose_to_mps(
    parts: Vec<Vec<RecordBatch>>,
    schema: SchemaRef,
    num_partitions: usize,
) -> Vec<MicroPartition> {
    if parts.is_empty() {
        return (0..num_partitions)
            .map(|_| MicroPartition::empty(Some(schema.clone())))
            .collect();
    }
    let mut iters: Vec<_> = parts.into_iter().map(|v| v.into_iter()).collect();
    (0..num_partitions)
        .map(|_| {
            let v: Vec<RecordBatch> = iters.iter_mut().map(|i| i.next().unwrap()).collect();
            MicroPartition::new_loaded(schema.clone(), Arc::new(v), None)
        })
        .collect()
}

// ---------- worker harness ----------

fn run_worker(
    strategy: Strategy,
    morsels: Vec<MicroPartition>,
    num_partitions: usize,
    key_expr: &[BoundExpr],
) -> DaftResult<(Vec<Vec<MicroPartition>>, Timings)> {
    match strategy {
        Strategy::Baseline => run_baseline(morsels, num_partitions, key_expr),
        Strategy::PerMorselRadix => run_per_morsel_radix(morsels, num_partitions, key_expr),
        Strategy::Defer => run_defer(morsels, num_partitions, key_expr),
        Strategy::Accumulate(t) => {
            run_accumulate(morsels, num_partitions, key_expr, t, FlushKind::Take)
        }
        Strategy::AccumulateScatter(t) => {
            run_accumulate(morsels, num_partitions, key_expr, t, FlushKind::Scatter)
        }
        Strategy::AccumulateRadix(t) => {
            run_accumulate(morsels, num_partitions, key_expr, t, FlushKind::Radix)
        }
        Strategy::HashOnly => run_hash_only(morsels, num_partitions, key_expr),
    }
}

fn run_bench(
    strategy: Strategy,
    cfg: &BenchConfig,
    morsels: Vec<Vec<MicroPartition>>,
    num_partitions: usize,
    schema: SchemaRef,
) -> DaftResult<Timings> {
    let key_expr = vec![BoundExpr::new_unchecked(bound_col(
        0,
        Field::new("key", DataType::UInt64),
    ))];

    let t_wall = Instant::now();

    let handles: Vec<_> = morsels
        .into_iter()
        .map(|worker_morsels| {
            let key_expr = key_expr.clone();
            std::thread::spawn(move || {
                run_worker(strategy, worker_morsels, num_partitions, &key_expr)
            })
        })
        .collect();

    let mut worker_results: Vec<(Vec<Vec<MicroPartition>>, Timings)> = Vec::new();
    for h in handles {
        let res = h.join().expect("worker panicked")?;
        worker_results.push(res);
    }

    // Finalize: flatten per partition, then concat per partition (in parallel across partitions).
    let mut t = worker_results
        .iter()
        .map(|(_, t)| *t)
        .fold(Timings::default(), |a, b| a + b);

    let mut per_partition: Vec<Vec<MicroPartition>> =
        (0..num_partitions).map(|_| Vec::new()).collect();
    for (worker_pp, _) in worker_results {
        for (i, mut chunk) in worker_pp.into_iter().enumerate() {
            per_partition[i].append(&mut chunk);
        }
    }

    // Per-partition concat (in parallel across partitions, mirroring the Ray finalize path).
    let t_fconcat = Instant::now();
    let concat_handles: Vec<_> = per_partition
        .into_iter()
        .map(|data| {
            let schema = schema.clone();
            std::thread::spawn(move || -> DaftResult<MicroPartition> {
                if data.is_empty() {
                    return Ok(MicroPartition::empty(Some(schema)));
                }
                let together = MicroPartition::concat(data)?;
                let concated = together.concat_or_get()?;
                Ok(MicroPartition::new_loaded(
                    schema,
                    Arc::new(if let Some(t) = concated { vec![t] } else { vec![] }),
                    None,
                ))
            })
        })
        .collect();
    let mut final_partitions = Vec::with_capacity(num_partitions);
    for h in concat_handles {
        final_partitions.push(h.join().expect("concat panicked")?);
    }
    t.finalize_concat.add(t_fconcat.elapsed());

    // Sanity-check total row count.
    let total_rows: usize = final_partitions.iter().map(|p| p.len()).sum();
    t.wall = t_wall.elapsed();

    // Stash row count in a phase we don't otherwise use, so we can sanity-check after.
    // (Encode in `count` field of a placeholder phase — not strictly needed but useful.)
    let _ = total_rows;

    Ok(t)
}

// ---------- printing ----------

fn print_timings(label: &str, t: &Timings, cfg: &BenchConfig) {
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    let avg_us = |p: Phase| {
        if p.count == 0 {
            0.0
        } else {
            p.total.as_secs_f64() * 1e6 / p.count as f64
        }
    };

    println!(
        "  {label:>14}  wall {:>7.1} ms | sink {:>7.1} ms | hash {:>5.1} | rem {:>5.1} | concat_buf {:>5.1} | take {:>6.1} | push {:>4.1} | accum_concat {:>5.1} | fin_take {:>6.1} | fin_concat {:>6.1} | per-take {:>7.1} us",
        ms(t.wall),
        ms(t.sink_total.total) / cfg.num_workers as f64,
        ms(t.hash.total) / cfg.num_workers as f64,
        ms(t.rem.total) / cfg.num_workers as f64,
        ms(t.concat_morsels.total) / cfg.num_workers as f64,
        ms(t.take_loop.total) / cfg.num_workers as f64,
        ms(t.push.total) / cfg.num_workers as f64,
        ms(t.accum_concat.total) / cfg.num_workers as f64,
        ms(t.finalize_take.total) / cfg.num_workers as f64,
        ms(t.finalize_concat.total),
        avg_us(t.take_loop) + avg_us(t.finalize_take),
    );
}

// ---------- main ----------

fn main() -> DaftResult<()> {
    let cfg = parse_args();

    // Build schema once.
    let mut fields = vec![Field::new("key", DataType::UInt64)];
    for i in 0..cfg.payload_cols {
        fields.push(Field::new(format!("v{i}"), DataType::UInt64));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let morsel_sizes = [MorselSize::Small, MorselSize::Medium, MorselSize::Large];
    let part_counts = [PartCount::Small, PartCount::Medium, PartCount::Large];
    let mib = 1024 * 1024;
    let strategies: Vec<Strategy> = vec![
        Strategy::Baseline,                 // per-morsel + N-take    (no opt)
        Strategy::PerMorselRadix,           // per-morsel + radix     (kernel only)
        Strategy::Accumulate(4 * mib),      // buffered + N-take      (buffering only)
        Strategy::AccumulateRadix(4 * mib), // buffered + radix       (current best)
    ];

    println!(
        "config: workers={}, morsels/worker={}, payload_cols={}",
        cfg.num_workers, cfg.morsels_per_worker, cfg.payload_cols,
    );
    println!();

    for msize in morsel_sizes {
        for pcount in part_counts {
            let rows = msize.rows(cfg.payload_cols);
            let bytes_per = rows * 8 * (1 + cfg.payload_cols);
            let total_rows = rows * cfg.morsels_per_worker * cfg.num_workers;
            let total_bytes = bytes_per * cfg.morsels_per_worker * cfg.num_workers;
            println!(
                "===== morsel_size={} ({} rows / {:.1} KiB)  num_partitions={} ({})  total={:.1} MiB =====",
                msize.name(),
                rows,
                bytes_per as f64 / 1024.0,
                pcount.name(),
                pcount.count(),
                total_bytes as f64 / (1024.0 * 1024.0),
            );

            for strat in &strategies {
                let morsels = gen_morsels(&cfg, msize, schema.clone());
                let t = run_bench(*strat, &cfg, morsels, pcount.count(), schema.clone())?;
                print_timings(&strat.name(), &t, &cfg);
            }
            let _ = total_rows;
            println!();
        }
    }

    Ok(())
}

fn parse_args() -> BenchConfig {
    let mut num_workers: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut morsels_per_worker: usize = 64;
    let mut payload_cols: usize = 7; // matches typical TPC-H lineitem-ish width
    let mut accum_threshold_bytes: usize = 16 * 1024 * 1024;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let val = args.next().unwrap_or_else(|| panic!("missing value for {arg}"));
        match arg.as_str() {
            "--workers" => num_workers = val.parse().unwrap(),
            "--morsels-per-worker" => morsels_per_worker = val.parse().unwrap(),
            "--payload-cols" => payload_cols = val.parse().unwrap(),
            "--accum-kib" => accum_threshold_bytes = val.parse::<usize>().unwrap() * 1024,
            _ => panic!("unknown arg: {arg}"),
        }
    }
    BenchConfig {
        num_workers,
        morsels_per_worker,
        payload_cols,
        accum_threshold_bytes,
    }
}
