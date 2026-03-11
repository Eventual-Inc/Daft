use std::cmp::Ordering;

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::full::FullNull,
    datatypes::{DataType, Field, UInt64Array},
    kernels::search_sorted::build_partial_compare_with_nulls,
};

use crate::RecordBatch;

/// A boxed comparator that compares row indices from two tables.
/// Returns `None` when both rows have null keys (incomparable).
type CombinedComparator<'a> = Box<dyn Fn(usize, usize) -> Option<Ordering> + 'a>;

/// Build a combined comparator over all join-key columns.
/// Returns a closure that compares row `a_idx` from `left` against row `b_idx`
/// from `right`. Returns `None` when both rows have null keys (incomparable).
fn build_combined_comparator<'a>(
    left: &'a RecordBatch,
    right: &'a RecordBatch,
) -> DaftResult<CombinedComparator<'a>> {
    let mut cmp_list = Vec::with_capacity(left.num_columns());
    for (left_series, right_series) in left.columns.iter().zip(right.columns.iter()) {
        cmp_list.push(build_partial_compare_with_nulls(
            left_series.to_arrow()?.as_ref(),
            right_series.to_arrow()?.as_ref(),
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

pub fn merge_inner_join(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
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

    debug_assert!(
        left.columns
            .iter()
            .zip(right.columns.iter())
            .all(|(l, r)| l.data_type() == r.data_type()),
        "merge_inner_join: left and right key column types must match"
    );

    let combined_comparator = build_combined_comparator(left, right)?;

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
            UInt64Array::empty("left_indices", &DataType::UInt64),
            UInt64Array::empty("right_indices", &DataType::UInt64),
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
                        right_indices.extend(std::iter::repeat_n(
                            right_idx as u64,
                            left_idx - start_left_idx,
                        ));
                    }
                    // If there was a staged right-side run, then we know that all rows in the run is equal to this
                    // new left-side row, so we add all such pairs to the output indices without any extra comparisons.
                    MergeJoinState::StagedRightEqualRun(start_right_idx) => {
                        left_indices.extend(std::iter::repeat_n(
                            left_idx as u64,
                            right_idx - start_right_idx,
                        ));
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
                        if right_idx == right.len() - 1 =>
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
                            if left_idx == left.len() - 1 {
                                MergeJoinState::StagedLeftEqualRun(left_idx)
                            } else {
                                MergeJoinState::LeftEqualRun(left_idx)
                            }
                        } else if right_idx == right.len() - 1 {
                            MergeJoinState::StagedRightEqualRun(right_idx)
                        } else {
                            MergeJoinState::RightEqualRun(right_idx)
                        }
                    }
                };
                // Move the pointer forward for the appropriate side of the join.
                match state {
                    // If extending a left-side run or propagating an existing right-side run, move left pointer forward.
                    MergeJoinState::LeftEqualRun(_) | MergeJoinState::StagedRightEqualRun(_) => {
                        left_idx += 1;
                    }
                    // If extending a right-side run or propagating an existing left-side run, move right pointer forward.
                    MergeJoinState::RightEqualRun(_) | MergeJoinState::StagedLeftEqualRun(_) => {
                        right_idx += 1;
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
    let left_series = UInt64Array::from_vec("left_indices", left_indices);
    let right_series = UInt64Array::from_vec("right_indices", right_indices);
    Ok((left_series, right_series))
}
pub fn merge_left_join(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    let combined_comparator = build_combined_comparator(left, right)?;

    let mut left_indices = vec![];
    let mut right_indices = vec![];
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left.len() {
        if right_idx >= right.len() {
            left_indices.push(left_idx as u64);
            right_indices.push(u64::MAX);
            left_idx += 1;
            continue;
        }

        match combined_comparator(left_idx, right_idx) {
            Some(Ordering::Less) => {
                left_indices.push(left_idx as u64);
                right_indices.push(u64::MAX);
                left_idx += 1;
            }
            Some(Ordering::Greater) => {
                right_idx += 1;
            }
            Some(Ordering::Equal) => {
                let start_right_idx = right_idx;
                while right_idx < right.len()
                    && matches!(
                        combined_comparator(left_idx, right_idx),
                        Some(Ordering::Equal)
                    )
                {
                    left_indices.push(left_idx as u64);
                    right_indices.push(right_idx as u64);
                    right_idx += 1;
                }
                left_idx += 1;
                right_idx = start_right_idx;
            }
            None => {
                left_indices.push(left_idx as u64);
                right_indices.push(u64::MAX);
                left_idx += 1;
                right_idx += 1;
            }
        }
    }

    Ok((
        UInt64Array::from_vec("left_indices", left_indices),
        UInt64Array::from_iter(
            Field::new("right_indices", DataType::UInt64),
            right_indices
                .into_iter()
                .map(|i| if i == u64::MAX { None } else { Some(i) }),
        ),
    ))
}

pub fn merge_right_join(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    let (r_idx, l_idx) = merge_left_join(right, left)?;
    Ok((l_idx, r_idx))
}

pub fn merge_semi_join(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    let combined_comparator = build_combined_comparator(left, right)?;

    let mut left_indices = vec![];
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left.len() && right_idx < right.len() {
        match combined_comparator(left_idx, right_idx) {
            Some(Ordering::Less) => {
                left_idx += 1;
            }
            Some(Ordering::Greater) => {
                right_idx += 1;
            }
            Some(Ordering::Equal) => {
                left_indices.push(left_idx as u64);
                left_idx += 1;
            }
            None => {
                left_idx += 1;
                right_idx += 1;
            }
        }
    }

    Ok((
        UInt64Array::from_vec("left_indices", left_indices),
        UInt64Array::empty("right_indices", &DataType::UInt64),
    ))
}

pub fn merge_anti_join(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    let combined_comparator = build_combined_comparator(left, right)?;

    let mut left_indices = vec![];
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left.len() && right_idx < right.len() {
        match combined_comparator(left_idx, right_idx) {
            Some(Ordering::Less) => {
                left_indices.push(left_idx as u64);
                left_idx += 1;
            }
            Some(Ordering::Greater) => {
                right_idx += 1;
            }
            Some(Ordering::Equal) => {
                left_idx += 1;
            }
            None => {
                left_indices.push(left_idx as u64);
                left_idx += 1;
                right_idx += 1;
            }
        }
    }

    // Remaining left rows have no match
    while left_idx < left.len() {
        left_indices.push(left_idx as u64);
        left_idx += 1;
    }

    Ok((
        UInt64Array::from_vec("left_indices", left_indices),
        UInt64Array::empty("right_indices", &DataType::UInt64),
    ))
}

pub fn merge_full_join(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    let combined_comparator = build_combined_comparator(left, right)?;

    let mut left_indices = vec![];
    let mut right_indices = vec![];
    let mut left_idx = 0;
    let mut right_idx = 0;

    while left_idx < left.len() || right_idx < right.len() {
        if left_idx >= left.len() {
            left_indices.push(u64::MAX);
            right_indices.push(right_idx as u64);
            right_idx += 1;
            continue;
        }
        if right_idx >= right.len() {
            left_indices.push(left_idx as u64);
            right_indices.push(u64::MAX);
            left_idx += 1;
            continue;
        }

        match combined_comparator(left_idx, right_idx) {
            Some(Ordering::Less) => {
                left_indices.push(left_idx as u64);
                right_indices.push(u64::MAX);
                left_idx += 1;
            }
            Some(Ordering::Greater) => {
                left_indices.push(u64::MAX);
                right_indices.push(right_idx as u64);
                right_idx += 1;
            }
            Some(Ordering::Equal) => {
                let start_right_idx = right_idx;
                while right_idx < right.len()
                    && matches!(
                        combined_comparator(left_idx, right_idx),
                        Some(Ordering::Equal)
                    )
                {
                    left_indices.push(left_idx as u64);
                    right_indices.push(right_idx as u64);
                    right_idx += 1;
                }
                left_idx += 1;
                if left_idx < left.len()
                    && combined_comparator(left_idx, start_right_idx) == Some(Ordering::Equal)
                {
                    right_idx = start_right_idx;
                }
            }
            None => {
                left_indices.push(left_idx as u64);
                right_indices.push(u64::MAX);
                left_indices.push(u64::MAX);
                right_indices.push(right_idx as u64);
                left_idx += 1;
                right_idx += 1;
            }
        }
    }

    let left_series = UInt64Array::from_iter(
        Field::new("left_indices", DataType::UInt64),
        left_indices
            .into_iter()
            .map(|i| if i == u64::MAX { None } else { Some(i) }),
    );
    let right_series = UInt64Array::from_iter(
        Field::new("right_indices", DataType::UInt64),
        right_indices
            .into_iter()
            .map(|i| if i == u64::MAX { None } else { Some(i) }),
    );
    Ok((left_series, right_series))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::{
        datatypes::{DataType, Field, Int64Array, UInt64Array},
        prelude::Schema,
        series::IntoSeries,
    };

    use super::{
        merge_anti_join, merge_full_join, merge_inner_join, merge_left_join, merge_right_join,
        merge_semi_join,
    };
    use crate::RecordBatch;

    fn make_rb(name: &str, values: Vec<i64>) -> RecordBatch {
        let array = Int64Array::from_vec(name, values);
        let series = array.into_series();
        RecordBatch::from_nonempty_columns(vec![series]).unwrap()
    }

    /// Helper to create a nullable Int64 RecordBatch (with None values).
    fn make_nullable_rb(name: &str, values: Vec<Option<i64>>) -> RecordBatch {
        let field = Field::new(name, DataType::Int64);
        let array = Int64Array::from_iter(field.clone(), values.into_iter());
        let series = array.into_series();
        RecordBatch::from_nonempty_columns(vec![series]).unwrap()
    }

    /// Helper: collect UInt64Array values as Vec<Option<u64>> for easy comparison.
    fn collect_idx(arr: &UInt64Array) -> Vec<Option<u64>> {
        (0..arr.len()).map(|i| arr.get(i)).collect()
    }

    // ========== INNER JOIN ==========

    #[test]
    fn test_merge_inner_join_basic() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![2, 3, 4]);
        let (l_idx, r_idx) = merge_inner_join(&left, &right).unwrap();

        // 2 matches: (1,0) and (2,1)
        assert_eq!(l_idx.len(), 2, "inner join: 2 matches expected");
        assert_eq!(r_idx.len(), 2);
        assert_eq!(collect_idx(&l_idx), vec![Some(1), Some(2)]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0), Some(1)]);
    }

    #[test]
    fn test_merge_inner_join_no_match() {
        let left = make_rb("a", vec![1, 2]);
        let right = make_rb("a", vec![3, 4]);
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 0, "inner join: no matches expected");
    }

    #[test]
    fn test_merge_inner_join_all_match() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![1, 2, 3]);
        let (l_idx, r_idx) = merge_inner_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 3, "inner join: all 3 match");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2)]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0), Some(1), Some(2)]);
    }

    #[test]
    fn test_merge_inner_join_right_typo() {
        // Left=[1,1], Right=[1,1,1]. Cross product: 2*3=6
        let left = make_rb("a", vec![1, 1]);
        let right = make_rb("a", vec![1, 1, 1]);
        let (l_idx, r_idx) = merge_inner_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 6, "Expected 6 rows for 2x3 cross product");
        assert_eq!(r_idx.len(), 6);
        // L0×R0, L0×R1, L0×R2, L1×R0, L1×R1, L1×R2
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(0), Some(0), Some(1), Some(1), Some(1)]
        );
        assert_eq!(
            collect_idx(&r_idx),
            vec![Some(0), Some(1), Some(2), Some(0), Some(1), Some(2)]
        );
    }

    #[test]
    fn test_merge_inner_join_dropped_rows() {
        // Left=[1,1,2,2,3], Right=[1,2,2,3,3]
        // 1: 2*1=2, 2: 2*2=4, 3: 1*2=2 => total 8
        let left = make_rb("a", vec![1, 1, 2, 2, 3]);
        let right = make_rb("a", vec![1, 2, 2, 3, 3]);
        let (l_idx, r_idx) = merge_inner_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 8, "Expected 8 rows");
        assert_eq!(r_idx.len(), 8);
        // Merge join state machine uses LeftEqualRun optimization,
        // so iteration order within a cross-product group follows the run pattern:
        // 1: L0×R0, L1×R0; 2: L2×R1, L3×R1, L2×R2, L3×R2; 3: L4×R3, L4×R4
        assert_eq!(
            collect_idx(&l_idx),
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(2),
                Some(3),
                Some(4),
                Some(4)
            ]
        );
        assert_eq!(
            collect_idx(&r_idx),
            vec![
                Some(0),
                Some(0),
                Some(1),
                Some(1),
                Some(2),
                Some(2),
                Some(3),
                Some(4)
            ]
        );
    }

    // ========== LEFT JOIN ==========

    #[test]
    fn test_merge_left_join_basic() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![2, 4]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        // Row 0: left=1, no match -> (0, NULL)
        // Row 1: left=2, match right=0 -> (1, 0)
        // Row 2: left=3, no match -> (2, NULL)
        assert_eq!(l_idx.len(), 3, "left join: all left rows preserved");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2)]);
        assert_eq!(collect_idx(&r_idx), vec![None, Some(0), None]);
    }

    #[test]
    fn test_merge_left_join_no_right_matches() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![4, 5]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 3, "left join: all left rows preserved");
        assert_eq!(collect_idx(&r_idx), vec![None, None, None]);
    }

    #[test]
    fn test_merge_left_join_with_duplicates() {
        let left = make_rb("a", vec![1, 2, 2, 3]);
        let right = make_rb("a", vec![2, 2, 4]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        // Row 0: left=1, no match -> (0, NULL)
        // Row 1: left=2, matches right 0,1 -> (1,0), (1,1)
        // Row 2: left=2, matches right 0,1 -> (2,0), (2,1)
        // Row 3: left=3, no match -> (3, NULL)
        assert_eq!(l_idx.len(), 6, "left join with dups: 1+2*2+1=6");
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(1), Some(1), Some(2), Some(2), Some(3)]
        );
        assert_eq!(
            collect_idx(&r_idx),
            vec![None, Some(0), Some(1), Some(0), Some(1), None]
        );
    }

    // ========== RIGHT JOIN ==========

    #[test]
    fn test_merge_right_join_basic() {
        let left = make_rb("a", vec![1, 3]);
        let right = make_rb("a", vec![2, 3, 4]);
        let (l_idx, r_idx) = merge_right_join(&left, &right).unwrap();

        // Right join is flipped left join. All right rows preserved.
        // right=2: no match -> (NULL, 0)
        // right=3: match left=1 -> (1, 1)
        // right=4: no match -> (NULL, 2)
        assert_eq!(r_idx.len(), 3, "right join: all right rows preserved");
        assert_eq!(collect_idx(&l_idx), vec![None, Some(1), None]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0), Some(1), Some(2)]);
    }

    // ========== SEMI JOIN ==========

    #[test]
    fn test_merge_semi_join_basic() {
        let left = make_rb("a", vec![1, 2, 3, 4]);
        let right = make_rb("a", vec![2, 3, 5]);
        let (l_idx, r_idx) = merge_semi_join(&left, &right).unwrap();

        // Semi join: left rows that have ANY match in right. No duplicates.
        // left=2 matches, left=3 matches
        assert_eq!(l_idx.len(), 2, "semi join: 2 matching left rows");
        assert_eq!(collect_idx(&l_idx), vec![Some(1), Some(2)]);
        assert_eq!(r_idx.len(), 0, "semi join: right indices should be empty");
    }

    #[test]
    fn test_merge_semi_join_no_duplicates() {
        // Even if right has duplicates, semi returns each left row at most once
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![2, 2, 2]);
        let (l_idx, _r_idx) = merge_semi_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 1, "semi join: left=2 appears once");
        assert_eq!(collect_idx(&l_idx), vec![Some(1)]);
    }

    #[test]
    fn test_merge_semi_join_no_match() {
        let left = make_rb("a", vec![1, 2]);
        let right = make_rb("a", vec![3, 4]);
        let (l_idx, _r_idx) = merge_semi_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 0, "semi join: no matches");
    }

    // ========== ANTI JOIN ==========

    #[test]
    fn test_merge_anti_join_basic() {
        let left = make_rb("a", vec![1, 2, 3, 4]);
        let right = make_rb("a", vec![2, 3, 5]);
        let (l_idx, r_idx) = merge_anti_join(&left, &right).unwrap();

        // Anti join: left rows that have NO match in right
        // left=1 no match, left=4 no match
        assert_eq!(l_idx.len(), 2, "anti join: 2 non-matching left rows");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(3)]);
        assert_eq!(r_idx.len(), 0, "anti join: right indices should be empty");
    }

    #[test]
    fn test_merge_anti_join_all_match() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![1, 2, 3]);
        let (l_idx, _r_idx) = merge_anti_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 0, "anti join: all match, empty result");
    }

    #[test]
    fn test_merge_anti_join_none_match() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![4, 5, 6]);
        let (l_idx, _r_idx) = merge_anti_join(&left, &right).unwrap();

        assert_eq!(l_idx.len(), 3, "anti join: none match, all left returned");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2)]);
    }

    #[test]
    fn test_merge_anti_join_remaining_left() {
        // After right exhausts, remaining left rows are unmatched
        let left = make_rb("a", vec![1, 2, 5, 6, 7]);
        let right = make_rb("a", vec![2, 3]);
        let (l_idx, _r_idx) = merge_anti_join(&left, &right).unwrap();

        // left=1 (no match), left=5,6,7 (right exhausted)
        assert_eq!(l_idx.len(), 4, "anti join: 4 unmatched left rows");
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(2), Some(3), Some(4)]
        );
    }

    // ========== FULL JOIN ==========

    #[test]
    fn test_merge_full_join_duplicates() {
        let left = make_rb("a", vec![1, 2]);
        let right = make_rb("a", vec![2, 3]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();

        // 1 (L only), 2 (match), 3 (R only) => 3 rows
        assert_eq!(l_idx.len(), 3, "full join: 3 rows");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), None]);
        assert_eq!(collect_idx(&r_idx), vec![None, Some(0), Some(1)]);
    }

    #[test]
    fn test_merge_full_join_no_overlap() {
        let left = make_rb("a", vec![1, 2]);
        let right = make_rb("a", vec![3, 4]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();

        // All rows unmatched
        assert_eq!(l_idx.len(), 4, "full join: 4 rows, no overlap");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), None, None]);
        assert_eq!(collect_idx(&r_idx), vec![None, None, Some(0), Some(1)]);
    }

    #[test]
    fn test_merge_full_join_with_duplicates() {
        let left = make_rb("a", vec![1, 2, 2]);
        let right = make_rb("a", vec![2, 2, 3]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();

        // left=1: unmatched -> (0, NULL)
        // left=2 x right=2: 2*2=4 matches -> (1,0),(1,1),(2,0),(2,1)
        // right=3: unmatched -> (NULL, 2)
        // Total: 1+4+1=6
        assert_eq!(l_idx.len(), 6, "full join with dups: 6 rows");
        assert_eq!(r_idx.len(), 6);
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(1), Some(1), Some(2), Some(2), None]
        );
        assert_eq!(
            collect_idx(&r_idx),
            vec![None, Some(0), Some(1), Some(0), Some(1), Some(2)]
        );
    }

    // ========== NULL HANDLING ==========
    // NOTE: merge join assumes sorted input with nulls_first=false,
    // so NULLs sort LAST (after all non-null values).

    #[test]
    fn test_merge_inner_join_nulls_no_match() {
        // NULL keys should NOT match each other in inner join
        // Sorted with NULLs last: [1, 3, NULL]
        let left = make_nullable_rb("a", vec![Some(1), Some(3), None]);
        let right = make_nullable_rb("a", vec![Some(3), Some(4), None]);
        let (l_idx, r_idx) = merge_inner_join(&left, &right).unwrap();

        // Only left=3 matches right=3 -> 1 row. NULLs don't match.
        assert_eq!(
            l_idx.len(),
            1,
            "inner join with NULLs: only non-null key=3 matches"
        );
        assert_eq!(collect_idx(&l_idx), vec![Some(1)]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0)]);
    }

    #[test]
    fn test_merge_left_join_nulls() {
        // NULL keys don't match, but left rows are preserved
        // Sorted with NULLs last
        let left = make_nullable_rb("a", vec![Some(1), Some(3), None]);
        let right = make_nullable_rb("a", vec![Some(3), None]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        // left=1: no match -> (0, NULL)
        // left=3: match right=0 -> (1, 0)
        // left=NULL: no match (NULLs don't match) -> (2, NULL)
        assert_eq!(l_idx.len(), 3, "left join with NULLs: all left preserved");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2)]);
        assert_eq!(collect_idx(&r_idx), vec![None, Some(0), None]);
    }

    #[test]
    fn test_merge_semi_join_nulls() {
        // NULL keys don't match in semi join
        // Sorted with NULLs last
        let left = make_nullable_rb("a", vec![Some(2), Some(3), None]);
        let right = make_nullable_rb("a", vec![Some(2), None]);
        let (l_idx, _r_idx) = merge_semi_join(&left, &right).unwrap();

        // Only left=2 matches
        assert_eq!(l_idx.len(), 1, "semi join with NULLs: only key=2 matches");
        assert_eq!(collect_idx(&l_idx), vec![Some(0)]);
    }

    #[test]
    fn test_merge_anti_join_nulls() {
        // NULL keys don't match, so NULL left rows are "unmatched"
        // Sorted with NULLs last
        let left = make_nullable_rb("a", vec![Some(2), Some(3), None]);
        let right = make_nullable_rb("a", vec![Some(2), None]);
        let (l_idx, _r_idx) = merge_anti_join(&left, &right).unwrap();

        // left=2: matches -> excluded
        // left=3: no match -> included
        // left=NULL: no match (NULL doesn't match) -> included
        assert_eq!(
            l_idx.len(),
            2,
            "anti join with NULLs: 3 and NULL are unmatched"
        );
        assert_eq!(collect_idx(&l_idx), vec![Some(1), Some(2)]);
    }

    #[test]
    fn test_merge_full_join_nulls() {
        // NULL keys don't match in full join
        // Sorted with NULLs last
        let left = make_nullable_rb("a", vec![Some(2), None]);
        let right = make_nullable_rb("a", vec![Some(2), Some(3), None]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();

        // left=2 == right=2 -> (0, 0)
        // right=3: unmatched -> (NULL, 1)
        // left=NULL: unmatched -> (1, NULL)
        // right=NULL: unmatched -> (NULL, 2)
        // Total: 4 rows
        assert_eq!(l_idx.len(), 4, "full join with NULLs: 4 rows");
        assert_eq!(r_idx.len(), 4);
        assert_eq!(collect_idx(&l_idx), vec![Some(0), None, Some(1), None]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0), Some(1), None, Some(2)]);
    }

    // ========== MULTI-COLUMN KEYS ==========

    #[test]
    fn test_merge_inner_join_multi_column() {
        // Join on two columns: (a, b)
        let left = {
            let a = Int64Array::from_vec("a", vec![1, 1, 2, 2]);
            let b = Int64Array::from_vec("b", vec![10, 20, 10, 20]);
            RecordBatch::from_nonempty_columns(vec![a.into_series(), b.into_series()]).unwrap()
        };
        let right = {
            let a = Int64Array::from_vec("a", vec![1, 2, 2]);
            let b = Int64Array::from_vec("b", vec![20, 10, 30]);
            RecordBatch::from_nonempty_columns(vec![a.into_series(), b.into_series()]).unwrap()
        };
        let (l_idx, r_idx) = merge_inner_join(&left, &right).unwrap();

        // Matches: (1,20) at L1,R0 and (2,10) at L2,R1
        assert_eq!(l_idx.len(), 2, "multi-col inner join: 2 matches");
        assert_eq!(collect_idx(&l_idx), vec![Some(1), Some(2)]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0), Some(1)]);
    }

    // ========== BUG VERIFICATION TESTS ==========
    // These tests verify the correctness of the None branch in merge_left_join
    // and merge_full_join, where both left and right keys are NULL.
    // The None branch advances both left_idx and right_idx by 1.
    // We test with asymmetric null counts to verify no rows are lost.

    #[test]
    fn test_merge_left_join_nulls_asymmetric_more_left_nulls() {
        // left has 3 nulls, right has 1 null. All nulls are at the end (nulls_last).
        // Left join must preserve ALL left rows.
        let left = make_nullable_rb("a", vec![Some(1), None, None, None]);
        let right = make_nullable_rb("a", vec![Some(1), None]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        // left=1 matches right=1 -> (0, 0)
        // left=null[1]: None branch -> (1, NULL), both idx advance
        // left=null[2]: right exhausted -> (2, NULL)
        // left=null[3]: right exhausted -> (3, NULL)
        assert_eq!(
            l_idx.len(),
            4,
            "left join: all 4 left rows must be preserved"
        );
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(1), Some(2), Some(3)]
        );
        assert_eq!(collect_idx(&r_idx), vec![Some(0), None, None, None]);
    }

    #[test]
    fn test_merge_left_join_nulls_asymmetric_more_right_nulls() {
        // left has 1 null, right has 3 nulls.
        // Left join must preserve ALL left rows. Extra right nulls are irrelevant.
        let left = make_nullable_rb("a", vec![Some(1), None]);
        let right = make_nullable_rb("a", vec![Some(1), None, None, None]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        // left=1 matches right=1 -> (0, 0)
        // left=null: None branch -> (1, NULL), both idx advance
        // loop ends (left exhausted)
        assert_eq!(
            l_idx.len(),
            2,
            "left join: all 2 left rows must be preserved"
        );
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1)]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0), None]);
    }

    #[test]
    fn test_merge_left_join_nulls_both_sides_multiple() {
        // Both sides have multiple nulls.
        let left = make_nullable_rb("a", vec![Some(1), Some(2), None, None]);
        let right = make_nullable_rb("a", vec![Some(2), Some(3), None, None]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        // left=1: Less -> (0, NULL)
        // left=2 matches right=2 -> (1, 0)
        // left=null[2]: right at 3(non-null), comparator(null,3)=Greater -> right_idx=2
        //   then left=null vs right=null -> None -> (2, NULL), left_idx=3, right_idx=3
        // left=null[3]: right at null[3] -> None -> (3, NULL), left_idx=4, right_idx=4
        // loop ends
        assert_eq!(
            l_idx.len(),
            4,
            "left join: all 4 left rows must be preserved"
        );
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(1), Some(2), Some(3)]
        );
        assert_eq!(collect_idx(&r_idx), vec![None, Some(0), None, None]);
    }

    #[test]
    fn test_merge_left_join_all_nulls() {
        // ALL keys are null on both sides.
        let left = make_nullable_rb("a", vec![None, None, None]);
        let right = make_nullable_rb("a", vec![None, None]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();

        // None branch pairs off one-by-one: (0, NULL)(1, NULL) then right exhausted: (2, NULL)
        assert_eq!(
            l_idx.len(),
            3,
            "left join: all 3 left rows must be preserved"
        );
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2)]);
        assert_eq!(collect_idx(&r_idx), vec![None, None, None]);
    }

    #[test]
    fn test_merge_right_join_nulls() {
        // Right join with nulls. Delegates to merge_left_join with swapped args.
        // Must preserve ALL right rows.
        let left = make_nullable_rb("a", vec![Some(1), None, None]);
        let right = make_nullable_rb("a", vec![Some(1), Some(2), None]);
        let (l_idx, r_idx) = merge_right_join(&left, &right).unwrap();

        // After swap: merge_left_join(right, left)
        //   right=1 matches left=1 -> (0, 0) in swapped space
        //   right=2: no left match -> (1, NULL)
        //   right=null: None branch -> (2, NULL)
        // Unswap: all right rows preserved
        assert_eq!(
            r_idx.len(),
            3,
            "right join: all 3 right rows must be preserved"
        );
        assert_eq!(collect_idx(&r_idx), vec![Some(0), Some(1), Some(2)]);
        assert_eq!(collect_idx(&l_idx), vec![Some(0), None, None]);
    }

    #[test]
    fn test_merge_full_join_nulls_asymmetric_more_left() {
        // Full join: left has 3 nulls, right has 1 null. All rows must appear.
        let left = make_nullable_rb("a", vec![Some(1), None, None, None]);
        let right = make_nullable_rb("a", vec![Some(1), None]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();

        // left=1 == right=1 -> (0, 0)
        // None branch: (1, NULL) + (NULL, 1), both advance
        // left exhaustion loop: (2, NULL), (3, NULL)
        // Total: 5 rows
        assert_eq!(
            l_idx.len(),
            5,
            "full join: 1 match + 3 left null unmatched + 1 right null unmatched"
        );
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(1), None, Some(2), Some(3)]
        );
        assert_eq!(
            collect_idx(&r_idx),
            vec![Some(0), None, Some(1), None, None]
        );
    }

    #[test]
    fn test_merge_full_join_nulls_asymmetric_more_right() {
        // Full join: left has 1 null, right has 3 nulls. All rows must appear.
        let left = make_nullable_rb("a", vec![Some(1), None]);
        let right = make_nullable_rb("a", vec![Some(1), None, None, None]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();

        // left=1 == right=1 -> (0, 0)
        // None branch: (1, NULL) + (NULL, 1), both advance
        // right exhaustion loop: (NULL, 2), (NULL, 3)
        // Total: 5 rows
        assert_eq!(
            l_idx.len(),
            5,
            "full join: 1 match + 1 left null + 3 right null"
        );
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), Some(1), None, None, None]
        );
        assert_eq!(
            collect_idx(&r_idx),
            vec![Some(0), None, Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn test_merge_full_join_all_nulls() {
        // ALL keys are null. No matches. Every row is unmatched.
        let left = make_nullable_rb("a", vec![None, None, None]);
        let right = make_nullable_rb("a", vec![None, None]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();

        // None branch pairs: (0, NULL)+(NULL, 0), (1, NULL)+(NULL, 1)
        // Left exhaustion: (2, NULL)
        // Total: 5 rows
        assert_eq!(
            l_idx.len(),
            5,
            "full join all nulls: 3 left + 2 right = 5 unmatched"
        );
        assert_eq!(
            collect_idx(&l_idx),
            vec![Some(0), None, Some(1), None, Some(2)]
        );
        assert_eq!(
            collect_idx(&r_idx),
            vec![None, Some(0), None, Some(1), None]
        );
    }

    // ========== HELPERS (new) ==========

    fn make_empty_rb(name: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64)]));
        RecordBatch::empty(Some(schema))
    }

    fn make_multi_col_rb(names: &[&str], col_values: Vec<Vec<i64>>) -> RecordBatch {
        let columns: Vec<_> = names
            .iter()
            .zip(col_values.into_iter())
            .map(|(name, values)| Int64Array::from_vec(name, values).into_series())
            .collect();
        RecordBatch::from_nonempty_columns(columns).unwrap()
    }

    fn make_null_type_rb(name: &str, len: usize) -> RecordBatch {
        use daft_core::series::Series;
        let series = Series::full_null(name, &DataType::Null, len);
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Null)]));
        RecordBatch::new_with_size(schema, vec![series], len).unwrap()
    }

    // ========== EMPTY INPUT ==========

    #[test]
    fn test_merge_inner_join_left_empty() {
        let left = make_empty_rb("a");
        let right = make_rb("a", vec![1, 2, 3]);
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 0, "inner join: left empty → 0 rows");
    }

    #[test]
    fn test_merge_inner_join_right_empty() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_empty_rb("a");
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 0, "inner join: right empty → 0 rows");
    }

    #[test]
    fn test_merge_left_join_left_empty() {
        let left = make_empty_rb("a");
        let right = make_rb("a", vec![1, 2]);
        let (l_idx, _r_idx) = merge_left_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 0, "left join: left empty → 0 rows");
    }

    #[test]
    fn test_merge_left_join_right_empty() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_empty_rb("a");
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();
        assert_eq!(
            l_idx.len(),
            3,
            "left join: right empty → 3 rows (all left preserved)"
        );
        assert_eq!(collect_idx(&r_idx), vec![None, None, None]);
    }

    #[test]
    fn test_merge_full_join_both_empty() {
        let left = make_empty_rb("a");
        let right = make_empty_rb("a");
        let (l_idx, _r_idx) = merge_full_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 0, "full join: both empty → 0 rows");
    }

    #[test]
    fn test_merge_anti_join_right_empty() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_empty_rb("a");
        let (l_idx, _r_idx) = merge_anti_join(&left, &right).unwrap();
        assert_eq!(
            l_idx.len(),
            3,
            "anti join: right empty → all left rows unmatched"
        );
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2)]);
    }

    // ========== SINGLE ELEMENT ==========

    #[test]
    fn test_merge_inner_join_single_match() {
        let left = make_rb("a", vec![5]);
        let right = make_rb("a", vec![5]);
        let (l_idx, r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 1, "single match");
        assert_eq!(collect_idx(&l_idx), vec![Some(0)]);
        assert_eq!(collect_idx(&r_idx), vec![Some(0)]);
    }

    #[test]
    fn test_merge_inner_join_single_no_match() {
        let left = make_rb("a", vec![3]);
        let right = make_rb("a", vec![5]);
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 0, "single no match");
    }

    #[test]
    fn test_merge_left_join_single_no_match() {
        let left = make_rb("a", vec![3]);
        let right = make_rb("a", vec![5]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 1, "left join single no match");
        assert_eq!(collect_idx(&r_idx), vec![None]);
    }

    // ========== MULTI-COLUMN KEY ==========

    #[test]
    fn test_merge_left_join_multi_column() {
        let left = make_multi_col_rb(&["a", "b"], vec![vec![1, 1, 2], vec![10, 20, 10]]);
        let right = make_multi_col_rb(&["a", "b"], vec![vec![1, 2], vec![20, 30]]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 3, "multi-col left join: 3 rows");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2)]);
        assert_eq!(collect_idx(&r_idx), vec![None, Some(0), None]);
    }

    #[test]
    fn test_merge_right_join_multi_column() {
        let left = make_multi_col_rb(&["a", "b"], vec![vec![1, 1, 2], vec![10, 20, 10]]);
        let right = make_multi_col_rb(&["a", "b"], vec![vec![1, 2], vec![20, 30]]);
        let (l_idx, r_idx) = merge_right_join(&left, &right).unwrap();
        assert_eq!(
            r_idx.len(),
            2,
            "multi-col right join: 2 right rows preserved"
        );
        assert_eq!(collect_idx(&r_idx), vec![Some(0), Some(1)]);
        assert_eq!(collect_idx(&l_idx), vec![Some(1), None]);
    }

    #[test]
    fn test_merge_semi_join_multi_column() {
        let left = make_multi_col_rb(&["a", "b"], vec![vec![1, 1, 2], vec![10, 20, 10]]);
        let right = make_multi_col_rb(&["a", "b"], vec![vec![1, 2], vec![20, 30]]);
        let (l_idx, _r_idx) = merge_semi_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 1, "multi-col semi join: only (1,20) matches");
        assert_eq!(collect_idx(&l_idx), vec![Some(1)]);
    }

    #[test]
    fn test_merge_anti_join_multi_column() {
        let left = make_multi_col_rb(&["a", "b"], vec![vec![1, 1, 2], vec![10, 20, 10]]);
        let right = make_multi_col_rb(&["a", "b"], vec![vec![1, 2], vec![20, 30]]);
        let (l_idx, _r_idx) = merge_anti_join(&left, &right).unwrap();
        assert_eq!(
            l_idx.len(),
            2,
            "multi-col anti join: (1,10) and (2,10) don't match"
        );
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(2)]);
    }

    #[test]
    fn test_merge_full_join_multi_column() {
        let left = make_multi_col_rb(&["a", "b"], vec![vec![1, 1, 2], vec![10, 20, 10]]);
        let right = make_multi_col_rb(&["a", "b"], vec![vec![1, 2], vec![20, 30]]);
        let (l_idx, r_idx) = merge_full_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 4, "multi-col full join: 4 rows");
        assert_eq!(collect_idx(&l_idx), vec![Some(0), Some(1), Some(2), None]);
        assert_eq!(collect_idx(&r_idx), vec![None, Some(0), None, Some(1)]);
    }

    // ========== NULL TYPE COLUMN ==========

    #[test]
    fn test_merge_inner_join_null_type_column() {
        let left = make_null_type_rb("a", 2);
        let right = make_null_type_rb("a", 3);
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(
            l_idx.len(),
            0,
            "Null type column: no matches (comparator returns None)"
        );
    }

    #[test]
    fn test_merge_left_join_null_type_column() {
        let left = make_null_type_rb("a", 2);
        let right = make_null_type_rb("a", 3);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();
        assert_eq!(
            l_idx.len(),
            2,
            "Null type left join: all 2 left rows preserved"
        );
        assert_eq!(collect_idx(&r_idx), vec![None, None]);
    }

    // ========== IS_KEY_NULL INVARIANT ==========

    #[test]
    fn test_merge_left_join_non_null_keys_on_equal() {
        // When combined_comparator returns Equal, keys must be non-null,
        // so all matched pairs should have valid (non-null) indices.
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![1, 2, 3]);
        let (l_idx, r_idx) = merge_left_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 3, "3 matches expected");
        // Verify every matched pair has non-null indices
        for i in 0..l_idx.len() {
            assert!(
                l_idx.get(i).is_some(),
                "left idx should be non-null at match"
            );
            assert!(
                r_idx.get(i).is_some(),
                "right idx should be non-null at match"
            );
        }
    }

    // ========== LARGE DUPLICATES ==========

    #[test]
    fn test_merge_inner_join_many_duplicates() {
        let left = make_rb("a", vec![1; 100]);
        let right = make_rb("a", vec![1; 100]);
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 10000, "100×100 cross product = 10000 rows");
    }

    #[test]
    fn test_merge_full_join_many_duplicates() {
        let left = make_rb("a", vec![1; 50]);
        let right = make_rb("a", vec![1; 50]);
        let (l_idx, _r_idx) = merge_full_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 2500, "50×50 cross product = 2500 rows");
    }

    // ========== SORT INVARIANT / SHORT-CIRCUIT ==========

    #[test]
    fn test_merge_inner_join_disjoint_ranges_left_less() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![10, 20, 30]);
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 0, "disjoint ranges (left < right): 0 rows");
    }

    #[test]
    fn test_merge_inner_join_disjoint_ranges_right_less() {
        let left = make_rb("a", vec![10, 20, 30]);
        let right = make_rb("a", vec![1, 2, 3]);
        let (l_idx, _r_idx) = merge_inner_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 0, "disjoint ranges (right < left): 0 rows");
    }

    #[test]
    fn test_merge_inner_join_column_count_mismatch() {
        let left = make_rb("a", vec![1, 2]);
        let right = make_multi_col_rb(&["a", "b"], vec![vec![1, 2], vec![10, 20]]);
        let result = merge_inner_join(&left, &right);
        assert!(result.is_err(), "column count mismatch should error");
    }

    #[test]
    fn test_merge_inner_join_zero_columns() {
        let left = RecordBatch::empty(Some(Arc::new(Schema::empty())));
        let right = RecordBatch::empty(Some(Arc::new(Schema::empty())));
        let result = merge_inner_join(&left, &right);
        assert!(result.is_err(), "zero columns should error");
    }

    #[test]
    fn test_merge_semi_join_with_duplicates_right() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![2, 2, 2, 3, 3]);
        let (l_idx, _r_idx) = merge_semi_join(&left, &right).unwrap();
        assert_eq!(l_idx.len(), 2, "semi join: no duplicate left rows");
        assert_eq!(collect_idx(&l_idx), vec![Some(1), Some(2)]);
    }
}
