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

/// Sorted, deduplicated view of positional delete indices.
///
/// Callers typically pass sorted data, in which case this borrows. Hoist this out
/// of per-row-group loops: the sortedness probe is O(n), so normalizing once per
/// row group turns row group pruning into O(row groups x deletes).
pub fn normalize_deletes(delete_rows: &[i64]) -> Cow<'_, [i64]> {
    debug_assert!(
        delete_rows.iter().all(|&r| r >= 0),
        "delete_rows contains negative values"
    );
    if delete_rows.windows(2).any(|w| w[0] >= w[1]) {
        let mut sorted = delete_rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        Cow::Owned(sorted)
    } else {
        Cow::Borrowed(delete_rows)
    }
}

/// Number of deletes in the file-global range `[lo, hi)` of an already-normalized
/// slice. `sorted` must be sorted ascending and non-negative — otherwise the
/// `(r as usize)` comparison is not monotonic over it and the result is garbage.
fn count_deletes_in_sorted_range(sorted: &[i64], lo: usize, hi: usize) -> usize {
    if sorted.is_empty() || hi <= lo {
        return 0;
    }
    let start = sorted.partition_point(|&r| (r as usize) < lo);
    let end = sorted.partition_point(|&r| (r as usize) < hi);
    end - start
}

/// File-relative index of the first row of every row group.
pub fn row_group_file_starts(metadata: &ParquetMetaData) -> Vec<usize> {
    let mut starts = Vec::with_capacity(metadata.num_row_groups());
    let mut acc = 0usize;
    for rg_idx in 0..metadata.num_row_groups() {
        starts.push(acc);
        acc += metadata.row_group(rg_idx).num_rows() as usize;
    }
    starts
}

/// Rows in `rg_indices` that actually reach the consumer, i.e. physical rows
/// minus the positional deletes falling inside those row groups.
///
/// This is the same quantity the row-level path derives from
/// `RowSelection::row_count()`; keep the two in agreement.
pub fn visible_rows_in_row_groups(
    metadata: &ParquetMetaData,
    rg_indices: &[usize],
    delete_rows: Option<&[i64]>,
) -> usize {
    let physical: usize = rg_indices
        .iter()
        .map(|&i| metadata.row_group(i).num_rows() as usize)
        .sum();
    let Some(deletes) = delete_rows.filter(|d| !d.is_empty()) else {
        return physical;
    };
    let normalized = normalize_deletes(deletes);
    let starts = row_group_file_starts(metadata);
    let deleted: usize = rg_indices
        .iter()
        .map(|&i| {
            let start = starts[i];
            let end = start + metadata.row_group(i).num_rows() as usize;
            count_deletes_in_sorted_range(&normalized, start, end)
        })
        .sum();
    physical.saturating_sub(deleted)
}

/// Build a `RowSelection` for a single row group from Iceberg positional
/// delete indices.
pub fn build_single_rg_delete_selection(
    delete_rows: &[i64],
    rg_global_start: usize,
    rg_rows: usize,
) -> RowSelection {
    let normalized = normalize_deletes(delete_rows);

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

/// Validate & resolve user/ChunkSpec-requested row group indices to positional
/// indices into `metadata`. Preserves order and duplicates (`[1, 1, 1]` → three
/// entries), empty input → empty output, out-of-bounds/negative index → error.
///
/// Shared by the normal read path ([`prune_row_groups`]) and the count-pushdown
/// shortcut so both validate identically.
pub fn validate_requested_row_groups(
    metadata: &ParquetMetaData,
    requested_row_groups: &[i64],
    uri: &str,
) -> DaftResult<Vec<usize>> {
    let num_row_groups = metadata.num_row_groups();
    requested_row_groups
        .iter()
        .map(|&i| {
            // A negative `i` wraps to a large `usize` and is caught by the
            // bounds check below, matching the prior behavior of this path.
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
        .collect()
}

/// Single-source-of-truth RG-level pruning. Applies, in order:
/// - user-supplied `requested_row_groups` (validated against metadata)
/// - positional `start_offset`: drop RGs whose last row is at/before the offset
/// - `num_rows` cap (no-predicate only — with a predicate, rows survive the
///   limit only after filtering, so later RGs may still be needed)
/// - `predicate` stats: drop RGs the min/max stats prove can't match
///
/// `delete_rows` holds Iceberg positional deletes. Those rows never reach the
/// consumer, so they must not count against the `num_rows` budget — otherwise a
/// heavily-deleted RG exhausts the budget and later RGs are dropped, returning
/// fewer rows than requested.
///
/// Returns RG indices in original (file) order.
#[allow(clippy::too_many_arguments)]
pub fn prune_row_groups(
    metadata: &ParquetMetaData,
    requested_row_groups: Option<&[i64]>,
    start_offset: usize,
    num_rows: Option<usize>,
    delete_rows: Option<&[i64]>,
    predicate: Option<&ExprRef>,
    schema: &Schema,
    uri: &str,
) -> DaftResult<Vec<usize>> {
    let num_row_groups = metadata.num_row_groups();
    let candidates: Vec<usize> = match requested_row_groups {
        Some(rgs) => validate_requested_row_groups(metadata, rgs, uri)?,
        None => (0..num_row_groups).collect(),
    };

    // File-relative row starts for ALL RGs — start_offset is a file-level
    // skip, not relative to `candidates`.
    let rg_file_start = row_group_file_starts(metadata);

    // Normalize once, not once per candidate row group.
    let normalized_deletes = delete_rows.map(normalize_deletes);

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
        let visible_start = rg_start.max(start_offset);
        let deleted = normalized_deletes.as_deref().map_or(0, |d| {
            count_deletes_in_sorted_range(d, visible_start, rg_end)
        });
        let contrib = (rg_end - visible_start).saturating_sub(deleted);
        rows_remaining = rows_remaining.saturating_sub(contrib as i64);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::Schema;
    use parquet::{
        file::metadata::{FileMetaData, ParquetMetaData, RowGroupMetaData},
        schema::types::{SchemaDescriptor, Type},
    };

    use super::{
        count_deletes_in_sorted_range, normalize_deletes, prune_row_groups,
        visible_rows_in_row_groups,
    };

    /// `ParquetMetaData` with `rows_per_group.len()` row groups and no statistics.
    fn metadata_with_row_groups(rows_per_group: &[i64]) -> ParquetMetaData {
        let schema = Arc::new(Type::group_type_builder("schema").build().unwrap());
        let descr = Arc::new(SchemaDescriptor::new(schema));
        let row_groups = rows_per_group
            .iter()
            .map(|&n| {
                RowGroupMetaData::builder(descr.clone())
                    .set_num_rows(n)
                    .build()
                    .unwrap()
            })
            .collect();
        let file_metadata =
            FileMetaData::new(2, rows_per_group.iter().sum(), None, None, descr, None);
        ParquetMetaData::new(file_metadata, row_groups)
    }

    #[test]
    fn counts_deletes_inside_range_only() {
        let deletes = [0i64, 1, 2, 50, 51, 199];
        assert_eq!(count_deletes_in_sorted_range(&deletes, 0, 50), 3);
        assert_eq!(count_deletes_in_sorted_range(&deletes, 50, 100), 2);
        assert_eq!(count_deletes_in_sorted_range(&deletes, 100, 150), 0);
        assert_eq!(count_deletes_in_sorted_range(&deletes, 150, 200), 1);
    }

    #[test]
    fn range_bounds_are_half_open() {
        let deletes = [10i64];
        assert_eq!(count_deletes_in_sorted_range(&deletes, 10, 11), 1);
        assert_eq!(count_deletes_in_sorted_range(&deletes, 11, 20), 0);
        assert_eq!(count_deletes_in_sorted_range(&deletes, 0, 10), 0);
    }

    #[test]
    fn empty_or_inverted_range_counts_nothing() {
        assert_eq!(count_deletes_in_sorted_range(&[], 0, 100), 0);
        assert_eq!(count_deletes_in_sorted_range(&[5i64], 10, 10), 0);
        assert_eq!(count_deletes_in_sorted_range(&[5i64], 10, 5), 0);
    }

    #[test]
    fn unsorted_and_duplicate_deletes_are_normalized() {
        let deletes = [51i64, 0, 50, 0, 2, 1];
        let normalized = normalize_deletes(&deletes);
        assert_eq!(&*normalized, &[0i64, 1, 2, 50, 51]);
        assert_eq!(count_deletes_in_sorted_range(&normalized, 0, 50), 3);
        assert_eq!(count_deletes_in_sorted_range(&normalized, 50, 100), 2);
        // Already-sorted input is borrowed, not copied.
        assert!(matches!(
            normalize_deletes(&[0i64, 1, 2]),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn visible_rows_subtracts_deletes_in_selected_row_groups() {
        let metadata = metadata_with_row_groups(&[50, 50, 50, 50]);
        // 40 deletes in RG0, 1 in RG2.
        let deletes: Vec<i64> = (0..40).chain(std::iter::once(120)).collect();

        assert_eq!(visible_rows_in_row_groups(&metadata, &[0], None), 50);
        assert_eq!(
            visible_rows_in_row_groups(&metadata, &[0], Some(&deletes)),
            10
        );
        // RG1 has no deletes; RG2 has one.
        assert_eq!(
            visible_rows_in_row_groups(&metadata, &[1, 2], Some(&deletes)),
            99
        );
        assert_eq!(
            visible_rows_in_row_groups(&metadata, &[0, 1, 2, 3], Some(&deletes)),
            159
        );
    }

    #[test]
    fn limit_budget_counts_only_visible_rows() {
        let metadata = metadata_with_row_groups(&[50, 50, 50, 50]);
        let schema = Schema::empty();
        let deletes: Vec<i64> = (0..40).collect();

        // Without deletes a limit of 20 is satisfied by RG0 alone.
        let kept =
            prune_row_groups(&metadata, None, 0, Some(20), None, None, &schema, "t").unwrap();
        assert_eq!(kept, vec![0]);

        // RG0 only yields 10 visible rows, so RG1 is still needed.
        let kept = prune_row_groups(
            &metadata,
            None,
            0,
            Some(20),
            Some(&deletes),
            None,
            &schema,
            "t",
        )
        .unwrap();
        assert_eq!(kept, vec![0, 1]);
    }

    #[test]
    fn start_offset_and_deletes_compose() {
        let metadata = metadata_with_row_groups(&[50, 50, 50, 50]);
        let schema = Schema::empty();
        // Deletes straddle the offset: 20..29 are skipped by the offset anyway,
        // only 30..49 reduce what RG0 contributes.
        let deletes: Vec<i64> = (20..50).collect();

        // Offset 30 drops nothing wholesale (RG0 ends at 50 > 30), and RG0's
        // visible span [30, 50) is entirely deleted, so the budget is untouched
        // and later row groups must be kept.
        let kept = prune_row_groups(
            &metadata,
            None,
            30,
            Some(20),
            Some(&deletes),
            None,
            &schema,
            "t",
        )
        .unwrap();
        assert_eq!(kept, vec![0, 1]);

        // Same offset without deletes: RG0's 20 visible rows fill the budget.
        let kept =
            prune_row_groups(&metadata, None, 30, Some(20), None, None, &schema, "t").unwrap();
        assert_eq!(kept, vec![0]);
    }
}
