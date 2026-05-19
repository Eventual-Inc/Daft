use std::{borrow::Cow, collections::HashSet};

use arrow::array::Array;
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{
    Expr, ExprRef,
    common_treenode::{Transformed, TreeNode},
    expr::{Column, ResolvedColumn, UnresolvedColumn, bound_expr::BoundExpr},
    null_lit,
    optimization::get_required_columns,
};
use daft_stats::TruthValue;
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    file::metadata::ParquetMetaData,
};

use crate::statistics::row_group_metadata_to_table_stats;

/// Returns column names referenced by `predicate`, or `None` if the predicate
/// is not pushable (no columns, or any column is missing from `daft_schema`).
pub fn predicate_pushable_cols(
    predicate: &ExprRef,
    daft_schema: &Schema,
) -> Option<HashSet<String>> {
    let cols: Vec<String> = get_required_columns(predicate);
    if cols.is_empty() || cols.iter().any(|c| daft_schema.get_field(c).is_err()) {
        return None;
    }
    Some(cols.into_iter().collect())
}

/// Substitute null for any column reference not present in `schema` (Iceberg
/// schema evolution). Null propagates conservatively — never falsely excludes.
pub fn substitute_missing_cols(predicate: &ExprRef, schema: &Schema) -> DaftResult<ExprRef> {
    Ok(predicate
        .clone()
        .transform(|e| {
            if let Expr::Column(col) = e.as_ref() {
                let name = match col {
                    Column::Unresolved(UnresolvedColumn { name, .. })
                    | Column::Resolved(ResolvedColumn::Basic(name)) => name,
                    _ => return Ok(Transformed::no(e)),
                };
                if schema.get_field(name).is_err() {
                    return Ok(Transformed::yes(null_lit()));
                }
            }
            Ok(Transformed::no(e))
        })?
        .data)
}

/// Build a `RowSelection` that skips the first `offset` rows of an RG.
pub fn build_offset_row_selection(offset: usize, total_rows: usize) -> RowSelection {
    if offset >= total_rows {
        RowSelection::from(vec![RowSelector::skip(total_rows)])
    } else {
        RowSelection::from(vec![
            RowSelector::skip(offset),
            RowSelector::select(total_rows - offset),
        ])
    }
}

/// Build a `RowSelection` for a single row group from Iceberg positional
/// delete indices.
pub fn build_single_rg_delete_selection(
    delete_rows: &[i64],
    rg_global_start: usize,
    rg_rows: usize,
) -> RowSelection {
    debug_assert!(
        delete_rows.iter().all(|&r| r >= 0),
        "delete_rows contains negative values"
    );
    // Normalize: ensure sorted+unique (callers typically pass sorted data).
    let normalized: Cow<'_, [i64]> = if delete_rows.windows(2).any(|w| w[0] >= w[1]) {
        let mut sorted = delete_rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        Cow::Owned(sorted)
    } else {
        Cow::Borrowed(delete_rows)
    };

    let rg_end = rg_global_start + rg_rows;
    let lo = normalized.partition_point(|&r| (r as usize) < rg_global_start);
    let hi = normalized.partition_point(|&r| (r as usize) < rg_end);
    let rg_deletes = &normalized[lo..hi];

    if rg_deletes.is_empty() {
        return vec![RowSelector::select(rg_rows)].into();
    }
    let mut selectors = Vec::with_capacity(rg_deletes.len() * 2 + 1);
    let mut pos = 0usize;
    for &del in rg_deletes {
        let local = del as usize - rg_global_start;
        if local < pos {
            continue;
        }
        if local > pos {
            selectors.push(RowSelector::select(local - pos));
        }
        selectors.push(RowSelector::skip(1));
        pos = local + 1;
    }
    if pos < rg_rows {
        selectors.push(RowSelector::select(rg_rows - pos));
    }
    selectors.into()
}

/// Combine two optional `RowSelection`s via intersection.
pub fn combine_selections(
    a: Option<RowSelection>,
    b: Option<RowSelection>,
) -> Option<RowSelection> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.intersection(&b)),
        (a @ Some(_), None) | (None, a @ Some(_)) => a,
        (None, None) => None,
    }
}

/// RLE-encode a boolean mask into a `RowSelection`.
pub fn bool_array_to_row_selection(mask: &arrow::array::BooleanArray) -> RowSelection {
    let mut selectors = Vec::new();
    let mut current_select = false;
    let mut current_count = 0usize;
    for i in 0..mask.len() {
        let val = mask.is_valid(i) && mask.value(i);
        if val == current_select {
            current_count += 1;
        } else {
            if current_count > 0 {
                selectors.push(if current_select {
                    RowSelector::select(current_count)
                } else {
                    RowSelector::skip(current_count)
                });
            }
            current_select = val;
            current_count = 1;
        }
    }
    if current_count > 0 {
        selectors.push(if current_select {
            RowSelector::select(current_count)
        } else {
            RowSelector::skip(current_count)
        });
    }
    selectors.into()
}

/// Compose a base selection (relative to the full RG) with a predicate
/// selection (relative to base-selected rows) into a final selection (relative
/// to the full RG).
pub fn refine_selection(base: &RowSelection, predicate_sel: &RowSelection) -> RowSelection {
    let base_selectors: Vec<RowSelector> = base.iter().copied().collect();
    let pred_selectors: Vec<RowSelector> = predicate_sel.iter().copied().collect();

    let mut result = Vec::new();
    let mut pred_idx = 0usize;
    let mut pred_remaining = if !pred_selectors.is_empty() {
        pred_selectors[0].row_count
    } else {
        0
    };

    for base_sel in &base_selectors {
        if base_sel.skip {
            result.push(RowSelector::skip(base_sel.row_count));
        } else {
            let mut remaining = base_sel.row_count;
            while remaining > 0 && pred_idx < pred_selectors.len() {
                let consume = remaining.min(pred_remaining);
                if pred_selectors[pred_idx].skip {
                    result.push(RowSelector::skip(consume));
                } else {
                    result.push(RowSelector::select(consume));
                }
                remaining -= consume;
                pred_remaining -= consume;
                if pred_remaining == 0 {
                    pred_idx += 1;
                    if pred_idx < pred_selectors.len() {
                        pred_remaining = pred_selectors[pred_idx].row_count;
                    }
                }
            }
            if remaining > 0 {
                result.push(RowSelector::skip(remaining));
            }
        }
    }
    result.into()
}

/// Single-source-of-truth RG-level pruning. Applies, in order:
/// - user-supplied `requested_row_groups` (validated against metadata)
/// - positional `start_offset`: drop RGs whose last row is at/before the offset
/// - `num_rows` cap (no-predicate only — with a predicate, rows survive the
///   limit only after filtering, so later RGs may still be needed)
/// - `predicate` stats: drop RGs the min/max stats prove can't match
///
/// Returns RG indices in original (file) order.
#[allow(clippy::too_many_arguments)]
pub fn prune_row_groups(
    metadata: &ParquetMetaData,
    requested_row_groups: Option<&[i64]>,
    start_offset: usize,
    num_rows: Option<usize>,
    predicate: Option<&ExprRef>,
    schema: &Schema,
    uri: &str,
) -> DaftResult<Vec<usize>> {
    let num_row_groups = metadata.num_row_groups();
    let candidates: Vec<usize> = if let Some(rgs) = requested_row_groups {
        rgs.iter()
            .map(|&i| {
                let idx = i as usize;
                if idx >= num_row_groups {
                    Err(common_error::DaftError::ValueError(format!(
                        "Row group index {} out of bounds for '{}' (has {} row groups)",
                        i, uri, num_row_groups
                    )))
                } else {
                    Ok(idx)
                }
            })
            .collect::<DaftResult<Vec<_>>>()?
    } else {
        (0..num_row_groups).collect()
    };

    // File-relative row starts for ALL RGs — start_offset is a file-level
    // skip, not relative to `candidates`.
    let mut rg_file_start = Vec::with_capacity(num_row_groups);
    let mut acc = 0usize;
    for rg_idx in 0..num_row_groups {
        rg_file_start.push(acc);
        acc += metadata.row_group(rg_idx).num_rows() as usize;
    }

    let mut rows_remaining: i64 = if predicate.is_none() {
        num_rows.map(|n| n as i64).unwrap_or(i64::MAX)
    } else {
        i64::MAX
    };

    let bound_pred = match predicate {
        Some(pred) => {
            let substituted = substitute_missing_cols(pred, schema)?;
            Some(BoundExpr::try_new(substituted, schema).map_err(|e| {
                common_error::DaftError::ValueError(format!(
                    "Failed to bind predicate for row group pruning on '{}': {}",
                    uri, e
                ))
            })?)
        }
        None => None,
    };

    let mut result = Vec::with_capacity(candidates.len());
    for rg_idx in candidates {
        let rg_rows = metadata.row_group(rg_idx).num_rows() as usize;
        let rg_start = rg_file_start[rg_idx];
        let rg_end = rg_start + rg_rows;
        if rg_end <= start_offset {
            continue;
        }
        if rows_remaining <= 0 {
            break;
        }
        if let Some(bound) = &bound_pred {
            // If stats are unavailable (or fail to convert), conservatively keep the RG.
            let keep = match row_group_metadata_to_table_stats(metadata.row_group(rg_idx), schema) {
                Ok(stats) => stats.eval_expression(bound)?.to_truth_value() != TruthValue::False,
                Err(_) => true,
            };
            if !keep {
                continue;
            }
        }
        result.push(rg_idx);
        let contrib = if rg_start < start_offset {
            rg_end - start_offset
        } else {
            rg_rows
        };
        rows_remaining = rows_remaining.saturating_sub(contrib as i64);
    }
    Ok(result)
}
