use std::cmp::Ordering;

use arrow::array::NullBufferBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{DataType, UInt64Array},
    join::JoinDirection,
    kernels::search_sorted::{DynPartialComparator, build_partial_compare_with_nulls},
    prelude::*,
};

use crate::RecordBatch;

/// Sort-merge two-pointer PIT (point-in-time / asof) join.
///
/// Both `left` and `right` must be sorted ascending by all columns
/// (by-keys first, on-key last). The first `num_by_keys` columns are
/// equality/grouping keys; the last column (index `num_by_keys`) is
/// the asof/ordering key.
///
/// Returns `(left_indices, right_indices)` where:
/// - `left_indices` is non-nullable: every left row appears in the output.
/// - `right_indices` is nullable: unmatched left rows have null.
pub fn asof_join(
    left: &RecordBatch,
    right: &RecordBatch,
    num_by_keys: usize,
    direction: JoinDirection,
    allow_exact_matches: bool,
) -> DaftResult<UInt64Array> {
    let total_key_cols = num_by_keys + 1;
    if left.num_columns() != total_key_cols || right.num_columns() != total_key_cols {
        return Err(DaftError::ValueError(format!(
            "Expected {} key columns ({}  by + 1 on), got left={}, right={}",
            total_key_cols,
            num_by_keys,
            left.num_columns(),
            right.num_columns()
        )));
    }

    // Early termination: left empty — return empty index arrays (nothing to match).
    if left.is_empty() {
        return Ok(UInt64Array::empty("right_indices", &DataType::UInt64));
    }

    let types_not_match = left
        .columns
        .iter()
        .zip(right.columns.iter())
        .any(|(l, r)| l.data_type() != r.data_type());
    if types_not_match {
        return Err(DaftError::SchemaMismatch(
            "Types between left and right key columns do not match".to_string(),
        ));
    }

    let by_comparator: DynPartialComparator =
        build_combined_comparator(left, right, 0, num_by_keys)?;

    let on_comparator = build_partial_compare_with_nulls(
        left.get_column(num_by_keys).to_arrow()?.as_ref(),
        right.get_column(num_by_keys).to_arrow()?.as_ref(),
        false,
    )?;

    let left_by_self_comparator: DynPartialComparator =
        build_combined_comparator(left, left, 0, num_by_keys)?;

    let left_on_nulls = left.get_column(num_by_keys).nulls().cloned();

    let results = match direction {
        JoinDirection::Backward => asof_backward(
            left,
            right,
            &by_comparator,
            &on_comparator,
            &left_by_self_comparator,
            left_on_nulls.as_ref(),
            allow_exact_matches,
        )?,
        _ => {
            return Err(DaftError::ComputeError(
                "Only backward direction is supported for asof join".to_string(),
            ));
        }
    };

    Ok(results)
}

fn build_combined_comparator(
    left: &RecordBatch,
    right: &RecordBatch,
    start: usize,
    end: usize,
) -> DaftResult<DynPartialComparator> {
    let mut cmp_list: Vec<DynPartialComparator> = Vec::with_capacity(end - start);
    for i in start..end {
        cmp_list.push(build_partial_compare_with_nulls(
            left.get_column(i).to_arrow()?.as_ref(),
            right.get_column(i).to_arrow()?.as_ref(),
            false,
        )?);
    }
    Ok(Box::new(
        move |a_idx: usize, b_idx: usize| -> Option<Ordering> {
            for comparator in &cmp_list {
                match comparator(a_idx, b_idx) {
                    Some(Ordering::Equal) => {}
                    other => return other,
                }
            }
            Some(Ordering::Equal)
        },
    ))
}

/// Backward PIT join: for each left row, find the largest right_on <= left_on
/// (or < if !allow_exact_matches) within the same by-group.
///
/// Both sides must be sorted ascending by (by_keys, on_key).
/// Runs in O(N + M) time.
///
/// Tracks two candidates per by-group:
/// - `best_strict`: best right row where right_on < left_on
/// - `last_equal`: best right row where right_on == some previous left_on
///
/// When left_on increases, `last_equal` is promoted to `best_strict` by
/// checking `on_comparator(left_idx, last_equal)`, reusing the existing
/// left-vs-right comparator instead of building a separate left-vs-left one.
fn asof_backward(
    left: &RecordBatch,
    right: &RecordBatch,
    by_comparator: &DynPartialComparator,
    on_comparator: &DynPartialComparator,
    left_by_self_comparator: &DynPartialComparator,
    left_on_nulls: Option<&arrow::buffer::NullBuffer>,
    allow_exact_matches: bool,
) -> DaftResult<UInt64Array> {
    let mut results = Vec::with_capacity(left.len());
    let mut results_valid = NullBufferBuilder::new(left.len());
    let mut right_idx = 0usize;
    let mut closest: Option<u64> = None;

    for left_idx in 0..left.len() {
        if left_idx > 0 && left_by_self_comparator(left_idx - 1, left_idx) != Some(Ordering::Equal)
        {
            closest = None;
        }

        // Null on-key can't match anything — emit null and skip.
        if left_on_nulls
            .as_ref()
            .is_some_and(|nb| !nb.is_valid(left_idx))
        {
            results.push(0);
            results_valid.append_null();
            continue;
        }

        // Advance right pointer to consume valid candidates for this left row.
        while right_idx < right.len() {
            match by_comparator(left_idx, right_idx) {
                Some(Ordering::Greater) => {
                    // Right by-keys < left by-keys: right is behind, skip.
                    closest = None;
                    right_idx += 1;
                }
                Some(Ordering::Equal) => match on_comparator(left_idx, right_idx) {
                    Some(Ordering::Greater) => {
                        // right_on < left_on: valid strict match.
                        closest = Some(right_idx as u64);
                        right_idx += 1;
                    }
                    Some(Ordering::Equal) => {
                        // right_on == left_on: exact match only.
                        if allow_exact_matches {
                            closest = Some(right_idx as u64);
                            right_idx += 1;
                        } else {
                            break;
                        }
                    }
                    Some(Ordering::Less) => {
                        break;
                    }
                    None => {
                        right_idx += 1;
                    }
                },
                Some(Ordering::Less) => {
                    break;
                }
                None => {
                    right_idx += 1;
                }
            }
        }

        match closest {
            Some(idx) => {
                results.push(idx);
                results_valid.append_non_null();
            }
            None => {
                results.push(0);
                results_valid.append_null();
            }
        }
    }

    UInt64Array::from_vec("right_indices", results).with_nulls(results_valid.finish())
}
