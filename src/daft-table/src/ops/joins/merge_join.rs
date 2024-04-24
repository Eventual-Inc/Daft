use std::cmp::Ordering;

use daft_core::{
    array::ops::full::FullNull,
    datatypes::{DataType, UInt64Array},
    kernels::search_sorted::build_partial_compare_with_nulls,
    series::{IntoSeries, Series},
};

use crate::Table;
use common_error::{DaftError, DaftResult};

/// A state machine for the below merge-join algorithm.
///
/// This state machine is in reference to a pair of left/right pointers into left/right tables of the merge-join.
///
/// Valid state transitions (initial state is Mismatch):
///
/// ANY -> BothNull
/// {Mismatch, BothNull, StagedLeftEqualRun, StagedRightEqualRun} -> Mismatch
/// ANY -> {LeftEqualRun, RightEqualRun}
/// LeftEqualRun -> StagedLeftEqualRun
/// RightEqualRun -> StagedRightEqualRun
#[derive(Debug)]
enum MergeJoinState {
    // Previous pair of rows were not equal.
    Mismatch,
    // Previous pair of rows were incomparable, i.e. for one or more join keys, they both had null values.
    BothNull,
    // Currently on a left-side equality run, where the left-side run started at the stored index and is relative to
    // a fixed right-side row.
    LeftEqualRun(usize),
    // Currently on a right-side equality run, where the right-side run started at the stored index and is relative to
    // a fixed left-side row.
    RightEqualRun(usize),
    // A staged left-side equality run starting at the stored index and ending at the current left pointer; this run
    // may be equal to one or more future right-side rows.
    StagedLeftEqualRun(usize),
    // A staged right-side equality run starting at the stored index and ending at the current right pointer; this run
    // may be equal to one or more future left-side rows.
    StagedRightEqualRun(usize),
}

pub fn merge_inner_join(left: &Table, right: &Table) -> DaftResult<(Series, Series)> {
    if left.num_columns() != right.num_columns() {
        return Err(DaftError::ValueError(format!(
            "Mismatch of join on clauses: left: {:?} vs right: {:?}",
            left.num_columns(),
            right.num_columns()
        )));
    }
    if left.num_columns() == 0 {
        return Err(DaftError::ValueError(
            "No columns were passed in to join on".to_string(),
        ));
    }

    // Short-circuit if any of the join keys are all-null (i.e. have the null dtype).
    let has_null_type = left.columns.iter().any(|s| s.data_type().is_null())
        || right.columns.iter().any(|s| s.data_type().is_null());
    if has_null_type {
        return Ok((
            UInt64Array::empty("left_indices", &DataType::UInt64).into_series(),
            UInt64Array::empty("right_indices", &DataType::UInt64).into_series(),
        ));
    }
    let types_not_match = left
        .columns
        .iter()
        .zip(right.columns.iter())
        .any(|(l, r)| l.data_type() != r.data_type());
    if types_not_match {
        return Err(DaftError::SchemaMismatch(
            "Types between left and right do not match".to_string(),
        ));
    }

    // TODO(Clark): If one of the tables is much larger than the other, iterate through smaller table while doing
    // binary search on the larger table.

    // Construct comparator over all join keys.
    let mut cmp_list = Vec::with_capacity(left.num_columns());
    for (left_series, right_series) in left.columns.iter().zip(right.columns.iter()) {
        cmp_list.push(build_partial_compare_with_nulls(
            left_series.to_arrow().as_ref(),
            right_series.to_arrow().as_ref(),
            false,
        )?);
    }
    let combined_comparator = |a_idx: usize, b_idx: usize| -> Option<Ordering> {
        for comparator in cmp_list.iter() {
            match comparator(a_idx, b_idx) {
                Some(Ordering::Equal) => continue,
                other => return other,
            }
        }
        Some(Ordering::Equal)
    };

    // Short-circuit if tables are empty or range-wise disjoint on join keys.
    if left.is_empty()
        || right.is_empty()
        || matches!(combined_comparator(left.len() - 1, 0), Some(Ordering::Less))
        || matches!(
            combined_comparator(0, right.len() - 1),
            Some(Ordering::Greater)
        )
    {
        return Ok((
            UInt64Array::empty("left_indices", &DataType::UInt64).into_series(),
            UInt64Array::empty("right_indices", &DataType::UInt64).into_series(),
        ));
    }

    // Perform the merge by building up left-side and right-side take index vectors.
    let mut left_indices = vec![];
    let mut right_indices = vec![];
    let mut left_idx = 0;
    let mut right_idx = 0;

    // The current state of the merge-join.
    let mut state = MergeJoinState::Mismatch;
    while left_idx < left.len() && right_idx < right.len() {
        match combined_comparator(left_idx, right_idx) {
            // Left row is less than right row, so need to move to next left row for potential match.
            Some(Ordering::Less) => {
                state = match state {
                    // If we previously had a right-side run of rows equal to a fixed left-side row, we move the right
                    // pointer back to the last row of that run, and stage the run for the comparison of said last row
                    // of the run with the next left-side row.
                    // If that next comparison comes out to be equal, we will do a bulk push of the right-side run with
                    // the new left-side row without having to compare the new left-side row with every row in the
                    // right-side run.
                    MergeJoinState::RightEqualRun(start_right_idx) => {
                        right_idx -= 1;
                        MergeJoinState::StagedRightEqualRun(start_right_idx)
                    }
                    // Any other previous states shouldn't matter going forward, so this is a plain mismatch.
                    _ => MergeJoinState::Mismatch,
                };
                left_idx += 1;
            }
            // Right row is less than left row, so need to move to next right row for potential match.
            Some(Ordering::Greater) => {
                state = match state {
                    // If we previously had a left-side run of rows equal to a fixed right-side row, we move the left
                    // pointer back to the last row of that run, and stage the run for the comparison of said last
                    // row of the run with the next right-side row.
                    // If that next comparison comes out to be equal, we will do a bulk push of the left-side run with
                    // the new right-side row without having to compare the new right-side row with every row in the
                    // left-side run.
                    MergeJoinState::LeftEqualRun(start_left_idx) => {
                        left_idx -= 1;
                        MergeJoinState::StagedLeftEqualRun(start_left_idx)
                    }
                    // Any other previous states shouldn't matter going forward, so this is a plain mismatch.
                    _ => MergeJoinState::Mismatch,
                };
                right_idx += 1;
            }
            // Left row is equal to the right row, so we need to add this pair of indices to the output indices.
            Some(Ordering::Equal) => {
                // First, handle past equal runs in bulk as a comparison-eliding optimization.
                match state {
                    // If there was a staged left-side run, then we know that all rows in the run is equal to this
                    // new right-side row, so we add all such pairs to the output indices without any extra comparisons.
                    MergeJoinState::StagedLeftEqualRun(start_left_idx) => {
                        left_indices.extend((start_left_idx..left_idx).map(|i| i as u64));
                        right_indices.extend(
                            std::iter::repeat(right_idx as u64).take(left_idx - start_left_idx),
                        );
                    }
                    // If there was a staged right-side run, then we know that all rows in the run is equal to this
                    // new left-side row, so we add all such pairs to the output indices without any extra comparisons.
                    MergeJoinState::StagedRightEqualRun(start_right_idx) => {
                        left_indices.extend(
                            std::iter::repeat(left_idx as u64).take(right_idx - start_right_idx),
                        );
                        right_indices.extend((start_right_idx..right_idx).map(|i| i as u64));
                    }
                    _ => {}
                }
                // Add current pointer pair to output indices.
                left_indices.push(left_idx as u64);
                right_indices.push(right_idx as u64);
                // Update state.
                state = match state {
                    // If already in a left-side equality run but we've reached the end of the left-side table,
                    // we can't extend the run anymore, so we stage it.
                    MergeJoinState::LeftEqualRun(start_left_idx) if left_idx == left.len() - 1 => {
                        MergeJoinState::StagedLeftEqualRun(start_left_idx)
                    }
                    // If already in a right-side equality run but we've reached the end of the right-side table,
                    // we can't extend the run anymore, so we stage it.
                    MergeJoinState::RightEqualRun(start_right_idx)
                        if right_idx == left.len() - 1 =>
                    {
                        MergeJoinState::StagedRightEqualRun(start_right_idx)
                    }
                    // If already in or just used a left-/right-side equality run and we're not at the end of the
                    // corresponding table, this equal comparison extends a current run or suggests we can keep
                    // applying a staged run; in either case, the state is unchanged.
                    MergeJoinState::LeftEqualRun(_)
                    | MergeJoinState::RightEqualRun(_)
                    | MergeJoinState::StagedLeftEqualRun(_)
                    | MergeJoinState::StagedRightEqualRun(_) => state,
                    // If coming from a non-active equality run, start a new run.
                    MergeJoinState::Mismatch | MergeJoinState::BothNull => {
                        // We assume that larger tables will have longer equality runs.
                        if left.len() >= right.len() {
                            MergeJoinState::LeftEqualRun(left_idx)
                        } else {
                            MergeJoinState::RightEqualRun(right_idx)
                        }
                    }
                };
                // Move the pointer forward for the appropriate side of the join.
                match state {
                    // If extending a left-side run or propagating an existing right-side run, move left pointer forward.
                    MergeJoinState::LeftEqualRun(_) | MergeJoinState::StagedRightEqualRun(_) => {
                        left_idx += 1
                    }
                    // If extending a right-side run or propagating an existing left-side run, move right pointer forward.
                    MergeJoinState::RightEqualRun(_) | MergeJoinState::StagedLeftEqualRun(_) => {
                        right_idx += 1
                    }
                    _ => unreachable!(),
                }
            }
            // Rows are not comparable, i.e. both rows are null for at least one of the join keys.
            None => {
                // Update the state.
                state = MergeJoinState::BothNull;
                // Advance past the nulls.
                left_idx += 1;
                right_idx += 1;
            }
        }
    }
    let left_series = UInt64Array::from(("left_indices", left_indices));
    let right_series = UInt64Array::from(("right_indices", right_indices));
    Ok((left_series.into_series(), right_series.into_series()))
}
