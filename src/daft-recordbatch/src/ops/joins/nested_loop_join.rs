use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{join::JoinSide, prelude::*};
use daft_dsl::expr::bound_expr::BoundExpr;

use crate::RecordBatch;

/// Tile size controls memory usage: each probe tile emits at most TILE_SIZE × |build| rows
/// before they are filtered down. With TILE_SIZE=512 and 300K build rows, worst-case
/// materialisation before filtering is 512 × 300K × ~50 bytes ≈ 7 GB — still large.
/// We set TILE_SIZE small enough that worst-case is under ~256 MB per tile.
/// 256 MB / (300K rows × 50 bytes/row) ≈ 17 rows; round up to 32 for cache alignment.
const TILE_SIZE: usize = 32;

/// Nested-loop inner join with a filter predicate.
///
/// For each tile of `probe_tbl` rows (outer loop) × all `build_tbl` rows (inner loop),
/// forms the cross-product, evaluates `filter`, and keeps only matching rows.
/// Memory usage is bounded by `TILE_SIZE × |build_tbl|` rows at any one time.
///
/// `build_side`: which side of the join the `build_tbl` represents (Left or Right).
/// The output schema is always `left_schema ∪ right_schema` (left columns first).
pub fn nested_loop_inner_join(
    probe_tbl: &RecordBatch,
    build_tbl: &RecordBatch,
    filter: &BoundExpr,
    build_side: JoinSide,
) -> DaftResult<RecordBatch> {
    // Output schema: left ∪ right (columns merged, no dedup needed for our use case
    // because the caller manages naming via projection)
    let output_schema = match build_side {
        JoinSide::Left => build_tbl.schema.union(&probe_tbl.schema)?,
        JoinSide::Right => probe_tbl.schema.union(&build_tbl.schema)?,
    };

    if probe_tbl.is_empty() || build_tbl.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(output_schema))));
    }

    // Collect (lidx, ridx) index pairs that pass the filter.
    let mut left_indices: Vec<u64> = Vec::new();
    let mut right_indices: Vec<u64> = Vec::new();

    let probe_len = probe_tbl.len();
    let build_len = build_tbl.len();

    let mut probe_start = 0;
    while probe_start < probe_len {
        let probe_end = (probe_start + TILE_SIZE).min(probe_len);
        let tile_size = probe_end - probe_start;

        // Build the tile cross-product: tile_size × build_len rows.
        //   probe repeated build_len times each (outer loop)
        //   build repeated tile_size times (inner loop)
        let probe_idx: Vec<u64> = (probe_start as u64..probe_end as u64)
            .flat_map(|i| std::iter::repeat_n(i, build_len))
            .collect();
        let build_idx: Vec<u64> = (0..build_len as u64)
            .cycle()
            .take(tile_size * build_len)
            .collect();

        let probe_idx_arr = UInt64Array::from_vec("probe_indices", probe_idx);
        let build_idx_arr = UInt64Array::from_vec("build_indices", build_idx);

        let (left_tbl, right_tbl) = match build_side {
            JoinSide::Left => (
                build_tbl.take(&build_idx_arr)?,
                probe_tbl.take(&probe_idx_arr)?,
            ),
            JoinSide::Right => (
                probe_tbl.take(&probe_idx_arr)?,
                build_tbl.take(&build_idx_arr)?,
            ),
        };

        // Form the cross-product batch for this tile.
        let tile_schema = left_tbl.schema.union(&right_tbl.schema)?;
        let mut tile_columns: Vec<Series> = Arc::unwrap_or_clone(left_tbl.columns)
            .into_iter()
            .map(|c| c.take_materialized_series())
            .collect();
        let right_columns: Vec<Series> = Arc::unwrap_or_clone(right_tbl.columns)
            .into_iter()
            .map(|c| c.take_materialized_series())
            .collect();
        tile_columns.extend(right_columns);

        let n_rows = tile_size * build_len;
        let tile_batch = RecordBatch::new_with_size(
            Arc::new(tile_schema),
            tile_columns,
            n_rows,
        )?;

        // Evaluate the filter on the tile.
        let mask = tile_batch.eval_expression(filter)?;
        let mask = mask.bool()?;

        // Collect the matching row indices back into the original tables.
        let (base_probe_start, base_build_start) = (probe_start as u64, 0u64);
        for row in 0..n_rows {
            if mask.get(row).is_some_and(|b| b) {
                let probe_row = base_probe_start + (row / build_len) as u64;
                let build_row = base_build_start + (row % build_len) as u64;
                match build_side {
                    JoinSide::Left => {
                        left_indices.push(build_row);
                        right_indices.push(probe_row);
                    }
                    JoinSide::Right => {
                        left_indices.push(probe_row);
                        right_indices.push(build_row);
                    }
                }
            }
        }

        probe_start = probe_end;
    }

    let n_out = left_indices.len();
    let lidx = UInt64Array::from_vec("left_indices", left_indices);
    let ridx = UInt64Array::from_vec("right_indices", right_indices);

    // Build output using take on the original (unindexed) tables.
    let (left_src, right_src) = match build_side {
        JoinSide::Left => (build_tbl, probe_tbl),
        JoinSide::Right => (probe_tbl, build_tbl),
    };

    let mut out_columns: Vec<Series> = Arc::unwrap_or_clone(left_src.take(&lidx)?.columns)
        .into_iter()
        .map(|c| c.take_materialized_series())
        .collect();
    let right_out: Vec<Series> = Arc::unwrap_or_clone(right_src.take(&ridx)?.columns)
        .into_iter()
        .map(|c| c.take_materialized_series())
        .collect();
    out_columns.extend(right_out);

    RecordBatch::new_with_size(Arc::new(output_schema), out_columns, n_out)
}

/// Accelerated nested-loop inner join using pre-computed R-tree candidate pairs.
///
/// Instead of forming the full O(N×M) cross-product and filtering, this function
/// accepts candidate `(probe_row, build_row)` index pairs produced by an R-tree
/// bbox query.  Only those candidates that pass `filter` appear in the output.
///
/// This reduces work to O(candidates × filter_cost), where `candidates ≪ N×M`
/// for typical spatial joins with selective bbox filtering.
pub fn nested_loop_inner_join_indexed(
    probe_tbl: &RecordBatch,
    build_tbl: &RecordBatch,
    filter: &BoundExpr,
    build_side: JoinSide,
    cand_probe_idx: &[u64],
    cand_build_idx: &[u64],
) -> DaftResult<RecordBatch> {
    debug_assert_eq!(cand_probe_idx.len(), cand_build_idx.len());

    let output_schema = match build_side {
        JoinSide::Left => build_tbl.schema.union(&probe_tbl.schema)?,
        JoinSide::Right => probe_tbl.schema.union(&build_tbl.schema)?,
    };

    if cand_probe_idx.is_empty() || probe_tbl.is_empty() || build_tbl.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(output_schema))));
    }

    let probe_idx_arr = UInt64Array::from_vec("probe_indices", cand_probe_idx.to_vec());
    let build_idx_arr = UInt64Array::from_vec("build_indices", cand_build_idx.to_vec());

    // Form the candidate sub-batch (only the pre-selected pairs, not the full cross-product).
    let (left_tbl, right_tbl) = match build_side {
        JoinSide::Left => (
            build_tbl.take(&build_idx_arr)?,
            probe_tbl.take(&probe_idx_arr)?,
        ),
        JoinSide::Right => (
            probe_tbl.take(&probe_idx_arr)?,
            build_tbl.take(&build_idx_arr)?,
        ),
    };

    let tile_schema = left_tbl.schema.union(&right_tbl.schema)?;
    let mut tile_columns: Vec<Series> = Arc::unwrap_or_clone(left_tbl.columns)
        .into_iter()
        .map(|c| c.take_materialized_series())
        .collect();
    tile_columns.extend(
        Arc::unwrap_or_clone(right_tbl.columns)
            .into_iter()
            .map(|c| c.take_materialized_series()),
    );

    let n_cands = cand_probe_idx.len();
    let tile_batch =
        RecordBatch::new_with_size(Arc::new(tile_schema), tile_columns, n_cands)?;

    // Evaluate the spatial (or arbitrary) filter on the candidate pairs.
    let mask = tile_batch.eval_expression(filter)?;
    let mask = mask.bool()?;

    // Collect indices of pairs that pass the filter (mapping back to original tables).
    let mut left_out: Vec<u64> = Vec::new();
    let mut right_out: Vec<u64> = Vec::new();
    for row in 0..n_cands {
        if mask.get(row).is_some_and(|b| b) {
            match build_side {
                JoinSide::Left => {
                    left_out.push(cand_build_idx[row]);
                    right_out.push(cand_probe_idx[row]);
                }
                JoinSide::Right => {
                    left_out.push(cand_probe_idx[row]);
                    right_out.push(cand_build_idx[row]);
                }
            }
        }
    }

    let n_out = left_out.len();
    if n_out == 0 {
        return Ok(RecordBatch::empty(Some(Arc::new(output_schema))));
    }

    let lidx = UInt64Array::from_vec("left_indices", left_out);
    let ridx = UInt64Array::from_vec("right_indices", right_out);

    let (left_src, right_src) = match build_side {
        JoinSide::Left => (build_tbl, probe_tbl),
        JoinSide::Right => (probe_tbl, build_tbl),
    };

    let mut out_columns: Vec<Series> = Arc::unwrap_or_clone(left_src.take(&lidx)?.columns)
        .into_iter()
        .map(|c| c.take_materialized_series())
        .collect();
    out_columns.extend(
        Arc::unwrap_or_clone(right_src.take(&ridx)?.columns)
            .into_iter()
            .map(|c| c.take_materialized_series()),
    );

    RecordBatch::new_with_size(Arc::new(output_schema), out_columns, n_out)
}
