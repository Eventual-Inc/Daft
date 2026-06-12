use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::UInt64Array;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{GrowableRecordBatch, ProbeState};

use crate::join::hash_join::HashJoinParams;

/// Threshold below which the per-row `GrowableRecordBatch::extend` path is faster
/// than bucketing + vectorized `RecordBatch::take`. Picked conservatively;
/// should be replaced with a value derived from a Rust-level micro-benchmark.
const TAKE_BATCH_MIN_MATCHES: usize = 1024;

/// Probe-side fan-out (matches per probe row) at or above which the vectorized
/// `take` path is preferred over the per-row Growable path. `take` has fixed
/// overhead from per-table `UInt64Array` construction + `RecordBatch::concat`;
/// it only pays off once each probe row produces enough matches.
const TAKE_BATCH_MIN_FANOUT: f64 = 3.0;

fn build_final_table(
    left_table: daft_recordbatch::RecordBatch,
    right_table: daft_recordbatch::RecordBatch,
    params: &HashJoinParams,
) -> DaftResult<daft_recordbatch::RecordBatch> {
    let common_join_keys: Vec<String> = params.common_join_cols.iter().cloned().collect();
    let left_non_join_columns: Vec<String> = params
        .left_schema
        .field_names()
        .filter(|c| !params.common_join_cols.contains(*c))
        .map(ToString::to_string)
        .collect();
    let right_non_join_columns: Vec<String> = params
        .right_schema
        .field_names()
        .filter(|c| !params.common_join_cols.contains(*c))
        .map(ToString::to_string)
        .collect();

    let join_keys_table = daft_recordbatch::get_columns_by_name(&left_table, &common_join_keys)?;
    let left_non_join_columns =
        daft_recordbatch::get_columns_by_name(&left_table, &left_non_join_columns)?;
    let right_non_join_columns =
        daft_recordbatch::get_columns_by_name(&right_table, &right_non_join_columns)?;
    join_keys_table
        .union(&left_non_join_columns)?
        .union(&right_non_join_columns)
}

pub(crate) fn probe_inner(
    input: &MicroPartition,
    probe_state: &ProbeState,
    params: &HashJoinParams,
) -> DaftResult<MicroPartition> {
    let input_tables = input.record_batches();
    let result_tables = input_tables
        .iter()
        .map(|input_table| {
            let join_keys = input_table.eval_expression_list(&params.probe_on)?;
            let idx_iter = probe_state.probe_indices(join_keys)?;

            // Materialize all matches into a flat Vec so we can (1) inspect the
            // total count to pick a build-side strategy and (2) bucket by
            // build_rb_idx for the take-based path. The probe-side hot loop
            // is a tight `Vec::push`, which the compiler lowers to the same
            // code as a `flat_map(...).collect()` form.
            // probe_idx is stored as u64 to avoid silent truncation on input
            // tables larger than u32::MAX rows.
            let mut matches: Vec<(u64, u64, u64)> = Vec::with_capacity(input_table.len());
            for (probe_idx, inner_iter) in idx_iter.enumerate() {
                if let Some(inner_iter) = inner_iter {
                    for (rb_idx, row_idx) in inner_iter {
                        matches.push((probe_idx as u64, rb_idx as u64, row_idx));
                    }
                }
            }
            let matches_len = matches.len();

            // Fan-out aware path selection.
            // - Below `TAKE_BATCH_MIN_MATCHES`: small total work, the per-row
            //   `GrowableRecordBatch::extend` path is faster.
            // - Below `TAKE_BATCH_MIN_FANOUT`: `take + concat` overhead exceeds
            //   the per-row dispatch cost.
            // - Otherwise: bucket by build table and use vectorized take.
            let probe_rows = input_table.len().max(1);
            let fanout = matches_len as f64 / probe_rows as f64;
            let use_batch =
                matches_len >= TAKE_BATCH_MIN_MATCHES && fanout >= TAKE_BATCH_MIN_FANOUT;

            let (left_table, right_table) = if use_batch {
                let build_tables = probe_state.get_record_batches();
                // Bucket matches by build table index. We store
                // `(probe_idx, row_idx)` pairs together so that when we later
                // walk the buckets in rb_idx order, both `probe_indices` and
                // the per-table row index arrays are produced in the same
                // grouped order. This keeps the probe-side `take` aligned
                // with the rb_idx-ordered build-side `concat` without
                // requiring an O(N log N) sort of `matches`.
                let mut per_table_indices: Vec<Vec<(u64, u64)>> =
                    vec![Vec::new(); build_tables.len()];
                for (probe_idx, rb_idx, row_idx) in matches {
                    per_table_indices[rb_idx as usize].push((probe_idx, row_idx));
                }

                let mut probe_indices = Vec::with_capacity(matches_len);
                let mut build_side_tables = Vec::new();
                for (rb_idx, bucket) in per_table_indices.into_iter().enumerate() {
                    if !bucket.is_empty() {
                        let mut row_indices = Vec::with_capacity(bucket.len());
                        for (probe_idx, row_idx) in bucket {
                            probe_indices.push(probe_idx);
                            row_indices.push(row_idx);
                        }
                        let idx_arr = UInt64Array::from_vec("", row_indices);
                        let taken = build_tables[rb_idx].take(&idx_arr)?;
                        build_side_tables.push(taken);
                    }
                }

                let build_side_table = if build_side_tables.is_empty() {
                    // The build side carries the schema of whichever input
                    // was built into the probe table, not the joined output
                    // schema; using `output_schema` here would cause a
                    // schema mismatch in `build_final_table`'s
                    // `get_columns_by_name` calls.
                    let build_schema = if params.build_on_left {
                        params.left_schema.clone()
                    } else {
                        params.right_schema.clone()
                    };
                    daft_recordbatch::RecordBatch::empty(Some(build_schema))
                } else if build_side_tables.len() == 1 {
                    build_side_tables.pop().unwrap()
                } else {
                    daft_recordbatch::RecordBatch::concat(&build_side_tables)?
                };

                let probe_indices_arr = UInt64Array::from_vec("", probe_indices);
                let probe_side_table = input_table.take(&probe_indices_arr)?;

                if params.build_on_left {
                    (build_side_table, probe_side_table)
                } else {
                    (probe_side_table, build_side_table)
                }
            } else {
                let build_side_tables =
                    probe_state.get_record_batches().iter().collect::<Vec<_>>();
                const DEFAULT_GROWABLE_SIZE: usize = 20;

                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    false,
                    matches_len.max(DEFAULT_GROWABLE_SIZE),
                )?;
                let mut probe_side_idxs = Vec::with_capacity(matches_len);

                for (probe_idx, rb_idx, row_idx) in matches {
                    build_side_growable.extend(rb_idx as usize, row_idx as usize, 1);
                    probe_side_idxs.push(probe_idx);
                }

                let build_side_table = build_side_growable.build()?;
                let probe_side_table = {
                    let indices_arr = UInt64Array::from_vec("", probe_side_idxs);
                    input_table.take(&indices_arr)?
                };

                if params.build_on_left {
                    (build_side_table, probe_side_table)
                } else {
                    (probe_side_table, build_side_table)
                }
            };

            build_final_table(left_table, right_table, params)
        })
        .collect::<DaftResult<Vec<_>>>()?;

    Ok(MicroPartition::new_loaded(
        params.output_schema.clone(),
        Arc::new(result_tables),
        None,
    ))
}
