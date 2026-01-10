use std::cmp::Ordering;

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::full::FullNull,
    datatypes::{DataType, UInt64Array},
    kernels::search_sorted::build_partial_compare_with_nulls,
};

use crate::RecordBatch;

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

    // Short-circuit if any of the join keys are all-null (i.e. have the null dtype).
    let has_null_type = left.columns.iter().any(|s| s.data_type().is_null())
        || right.columns.iter().any(|s| s.data_type().is_null());
    if has_null_type {
        return Ok((
            UInt64Array::empty("left_indices", &DataType::UInt64),
            UInt64Array::empty("right_indices", &DataType::UInt64),
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
        #[allow(deprecated, reason = "arrow2 migration")]
        cmp_list.push(build_partial_compare_with_nulls(
            left_series.to_arrow2().as_ref(),
            right_series.to_arrow2().as_ref(),
            false,
        )?);
    }
    let combined_comparator = |a_idx: usize, b_idx: usize| -> Option<Ordering> {
        for comparator in &cmp_list {
            match comparator(a_idx, b_idx) {
                Some(Ordering::Equal) => {}
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
    let left_series = UInt64Array::from(("left_indices", left_indices));
    let right_series = UInt64Array::from(("right_indices", right_indices));
    Ok((left_series, right_series))
}

pub fn merge_semi_join(left: &RecordBatch, right: &RecordBatch) -> DaftResult<UInt64Array> {
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

    // If any join key column is the null dtype, there can be no equality matches.
    let has_null_type = left.columns.iter().any(|s| s.data_type().is_null())
        || right.columns.iter().any(|s| s.data_type().is_null());
    if has_null_type {
        return Ok(UInt64Array::empty("left_indices", &DataType::UInt64));
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

    // Build comparator over all join keys.
    let mut cmp_list = Vec::with_capacity(left.num_columns());
    for (left_series, right_series) in left.columns.iter().zip(right.columns.iter()) {
        cmp_list.push(build_partial_compare_with_nulls(
            left_series.to_arrow2().as_ref(),
            right_series.to_arrow2().as_ref(),
            false,
        )?);
    }
    let combined_comparator = |a_idx: usize, b_idx: usize| -> Option<Ordering> {
        for comparator in &cmp_list {
            match comparator(a_idx, b_idx) {
                Some(Ordering::Equal) => {}
                other => return other,
            }
        }
        Some(Ordering::Equal)
    };

    // Short-circuit emptiness or disjoint ranges.
    if left.is_empty() || right.is_empty() {
        return Ok(UInt64Array::empty("left_indices", &DataType::UInt64));
    }
    if matches!(combined_comparator(left.len() - 1, 0), Some(Ordering::Less))
        || matches!(
            combined_comparator(0, right.len() - 1),
            Some(Ordering::Greater)
        )
    {
        return Ok(UInt64Array::empty("left_indices", &DataType::UInt64));
    }

    let mut out: Vec<u64> = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() && ri < right.len() {
        match combined_comparator(li, ri) {
            Some(Ordering::Less) => {
                // Left key < current right key => no match for this left row.
                li += 1;
            }
            Some(Ordering::Greater) => {
                // Advance right to catch up to left.
                ri += 1;
            }
            Some(Ordering::Equal) => {
                // Found a match for this join-key value.
                //
                // For SEMI join, each left row should be emitted at most once if it has any match on the right.
                // We can advance past the entire equality run on *both* sides to preserve monotonic progress and
                // avoid repeatedly scanning large right-side duplicate runs.

                // Emit all left duplicates equal to right[ri].
                let mut lrun = li;
                while lrun < left.len() {
                    match combined_comparator(lrun, ri) {
                        Some(Ordering::Equal) => {
                            out.push(lrun as u64);
                            lrun += 1;
                        }
                        _ => break,
                    }
                }
                li = lrun;

                // Skip all right duplicates for this join-key value.
                let mut rrun = ri + 1;
                while rrun < right.len() {
                    match combined_comparator(li.saturating_sub(1), rrun) {
                        Some(Ordering::Equal) => rrun += 1,
                        _ => break,
                    }
                }
                ri = rrun;
            }
            None => {
                // Incomparable (both null on some key) => not a match under SQL equality semantics.
                li += 1;
            }
        }
    }

    Ok(UInt64Array::from(("left_indices", out)))
}

pub fn merge_anti_join(left: &RecordBatch, right: &RecordBatch) -> DaftResult<UInt64Array> {
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

    let has_null_type = left.columns.iter().any(|s| s.data_type().is_null())
        || right.columns.iter().any(|s| s.data_type().is_null());
    if has_null_type {
        // No equality matches possible => all left rows are anti-joined.
        let idx: Vec<u64> = (0..left.len() as u64).collect();
        return Ok(UInt64Array::from(("left_indices", idx)));
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

    // Build comparator over all join keys.
    let mut cmp_list = Vec::with_capacity(left.num_columns());
    for (left_series, right_series) in left.columns.iter().zip(right.columns.iter()) {
        cmp_list.push(build_partial_compare_with_nulls(
            left_series.to_arrow2().as_ref(),
            right_series.to_arrow2().as_ref(),
            false,
        )?);
    }
    let combined_comparator = |a_idx: usize, b_idx: usize| -> Option<Ordering> {
        for comparator in &cmp_list {
            match comparator(a_idx, b_idx) {
                Some(Ordering::Equal) => {}
                other => return other,
            }
        }
        Some(Ordering::Equal)
    };

    // Short-circuit emptiness or disjoint ranges.
    if left.is_empty() {
        return Ok(UInt64Array::empty("left_indices", &DataType::UInt64));
    }

    // Check if any join key columns have nulls - if so, disable optimization
    // as null comparisons don't respect linear ordering
    let mut has_nulls_in_left = false;
    let mut has_nulls_in_right = false;
    for (left_series, right_series) in left.columns.iter().zip(right.columns.iter()) {
        let left_arrow = left_series.to_arrow2();
        let left_validity = left_arrow.validity();
        if left_validity.map(|v| v.unset_bits() > 0).unwrap_or(false) {
            has_nulls_in_left = true;
        }

        let right_arrow = right_series.to_arrow2();
        let right_validity = right_arrow.validity();
        if right_validity.map(|v| v.unset_bits() > 0).unwrap_or(false) {
            has_nulls_in_right = true;
        }

        // Early exit if we found nulls in either side
        if has_nulls_in_left && has_nulls_in_right {
            break;
        }
    }

    // Only apply short-circuit optimization if neither side has nulls
    // When there are nulls, the combined_comparator will return None for null comparisons,
    // and we cannot safely short-circuit as null should be included in anti-join results
    // This check ensures we only short-circuit when we have complete numerical ordering
    // that is not disturbed by null values
    if !right.is_empty() && !has_nulls_in_left && !has_nulls_in_right {
        let left_min_gt_right_max = matches!(
            combined_comparator(0, right.len() - 1),
            Some(Ordering::Greater)
        );
        let left_max_lt_right_min =
            matches!(combined_comparator(left.len() - 1, 0), Some(Ordering::Less));

        if left_min_gt_right_max || left_max_lt_right_min {
            // No overlap in join key ranges, so all left rows are anti-joined
            let idx: Vec<u64> = (0..left.len() as u64).collect();
            return Ok(UInt64Array::from(("left_indices", idx)));
        }
    }

    let mut out: Vec<u64> = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() && ri < right.len() {
        match combined_comparator(li, ri) {
            Some(Ordering::Less) => {
                // Left key < current right key => no right match for this left.
                out.push(li as u64);
                li += 1;
            }
            Some(Ordering::Greater) => {
                // Advance right to catch up to left.
                ri += 1;
            }
            Some(Ordering::Equal) => {
                // Found a match for this join-key value.
                //
                // For ANTI join, left rows that match any right row should be excluded.
                // Advance past equality runs on both sides to preserve monotonic progress and avoid repeatedly
                // scanning large right-side duplicate runs.

                // Skip all left duplicates equal to right[ri] (they are not anti-joined).
                let mut lrun = li;
                while lrun < left.len() {
                    match combined_comparator(lrun, ri) {
                        Some(Ordering::Equal) => lrun += 1,
                        _ => break,
                    }
                }
                li = lrun;

                // Skip all right duplicates for this join-key value.
                let mut rrun = ri + 1;
                while rrun < right.len() {
                    match combined_comparator(li.saturating_sub(1), rrun) {
                        Some(Ordering::Equal) => rrun += 1,
                        _ => break,
                    }
                }
                ri = rrun;
            }
            None => {
                // Incomparable (both null on some key): treated as no match => include left row.
                out.push(li as u64);
                li += 1;
            }
        }
    }
    // Any remaining left rows after right is exhausted are anti-joined.
    while li < left.len() {
        out.push(li as u64);
        li += 1;
    }

    Ok(UInt64Array::from(("left_indices", out)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_arrow::array::Int32Array as ArrowInt32Array;
    use daft_core::{
        datatypes::{DataType, Field},
        prelude::AsArrow,
        series::Series,
    };

    use super::*;

    fn rb_from_opt_i32(name: &str, vals: &[Option<i32>]) -> RecordBatch {
        let field = Arc::new(Field::new(name, DataType::Int32));
        let arr = Box::new(ArrowInt32Array::from(vals)) as Box<dyn daft_arrow::array::Array>;
        let col = Series::from_arrow(field, arr.into()).unwrap();
        RecordBatch::from_nonempty_columns(vec![col]).unwrap()
    }

    fn collect_indices(arr: &UInt64Array) -> Vec<u64> {
        arr.as_arrow2().iter().flatten().copied().collect()
    }

    // SEMI JOIN TESTS
    #[test]
    fn semi_empty_right_returns_empty() {
        let left = rb_from_opt_i32("key", &[Some(1), Some(2), Some(3)]);
        let right = rb_from_opt_i32("key", &[]);
        let out = merge_semi_join(&left, &right).unwrap();
        assert_eq!(out.len(), 0);
        assert!(collect_indices(&out).is_empty());
    }

    #[test]
    fn semi_disjoint_ranges_return_empty() {
        // left max < right min
        let left1 = rb_from_opt_i32("key", &[Some(1), Some(2)]);
        let right1 = rb_from_opt_i32("key", &[Some(10), Some(11)]);
        let out1 = merge_semi_join(&left1, &right1).unwrap();
        assert!(collect_indices(&out1).is_empty());

        // left min > right max
        let left2 = rb_from_opt_i32("key", &[Some(10), Some(11)]);
        let right2 = rb_from_opt_i32("key", &[Some(1), Some(2), Some(3)]);
        let out2 = merge_semi_join(&left2, &right2).unwrap();
        assert!(collect_indices(&out2).is_empty());
    }

    #[test]
    fn semi_includes_all_left_duplicates_that_match() {
        let left = rb_from_opt_i32("key", &[Some(1), Some(1), Some(2), Some(3), Some(3)]);
        let right = rb_from_opt_i32("key", &[Some(1), Some(3)]);
        let out = merge_semi_join(&left, &right).unwrap();
        assert_eq!(collect_indices(&out), vec![0, 1, 3, 4]);
    }

    #[test]
    fn semi_duplicate_keys_on_both_sides_does_not_hang() {
        // Regression test for pointer advancement when both sides have duplicate keys.
        // Inputs must be sorted.
        let left = rb_from_opt_i32("key", &[Some(1), Some(1), Some(2)]);
        let right = rb_from_opt_i32("key", &[Some(1), Some(1), Some(3)]);
        let out = merge_semi_join(&left, &right).unwrap();
        assert_eq!(collect_indices(&out), vec![0, 1]);
    }

    #[test]
    fn semi_skips_large_right_duplicate_run() {
        // If right has a large duplicate run, we should not repeatedly scan it one-by-one for each left key.
        // This test is primarily a correctness + regression guard for advancing `ri` past the right equality run.
        let left = rb_from_opt_i32("key", &[Some(1), Some(2)]);

        let mut right_vals: Vec<Option<i32>> = vec![Some(1); 1024];
        right_vals.push(Some(2));
        let right = rb_from_opt_i32("key", &right_vals);

        let out = merge_semi_join(&left, &right).unwrap();
        assert_eq!(collect_indices(&out), vec![0, 1]);
    }

    #[test]
    fn semi_treats_both_null_as_no_match() {
        // Inputs must be sorted ascending with nulls last (as sort_merge_join provides)
        let left = rb_from_opt_i32("key", &[Some(1), Some(2), None, None]);
        let right = rb_from_opt_i32("key", &[Some(2), None]);
        let out = merge_semi_join(&left, &right).unwrap();
        // Only the value 2 matches; nulls and 1 do not. Index 1 corresponds to value 2
        assert_eq!(collect_indices(&out), vec![1]);
    }

    #[test]
    fn semi_type_mismatch_errors() {
        let field_l = Arc::new(Field::new("key", DataType::Int32));
        let field_r = Arc::new(Field::new("key", DataType::Int64));
        let arr_l = Box::new(ArrowInt32Array::from(&[Some(1), Some(2)]))
            as Box<dyn daft_arrow::array::Array>;
        let col_l = Series::from_arrow(field_l, arr_l.into()).unwrap();
        let left = RecordBatch::from_nonempty_columns(vec![col_l]).unwrap();

        // Build an Int64 arrow array via arrow2
        let arr_r = Box::new(daft_arrow::array::Int64Array::from(&[
            Some(1i64),
            Some(3i64),
        ])) as Box<dyn daft_arrow::array::Array>;
        let col_r = Series::from_arrow(field_r, arr_r.into()).unwrap();
        let right = RecordBatch::from_nonempty_columns(vec![col_r]).unwrap();

        let err = merge_semi_join(&left, &right).err().unwrap();
        let msg = format!("{}", err);
        assert!(msg.contains("Types between left and right do not match"));
    }

    // ANTI JOIN TESTS
    #[test]
    fn anti_empty_right_returns_all_left() {
        let left = rb_from_opt_i32("key", &[Some(1), Some(2), Some(3)]);
        let right = rb_from_opt_i32("key", &[]);
        let out = merge_anti_join(&left, &right).unwrap();
        assert_eq!(collect_indices(&out), vec![0, 1, 2]);
    }

    #[test]
    fn anti_disjoint_ranges_return_all_left() {
        let left1 = rb_from_opt_i32("key", &[Some(1), Some(2)]);
        let right1 = rb_from_opt_i32("key", &[Some(10), Some(11)]);
        let out1 = merge_anti_join(&left1, &right1).unwrap();
        assert_eq!(collect_indices(&out1), vec![0, 1]);

        let left2 = rb_from_opt_i32("key", &[Some(10), Some(11)]);
        let right2 = rb_from_opt_i32("key", &[Some(1), Some(2), Some(3)]);
        let out2 = merge_anti_join(&left2, &right2).unwrap();
        assert_eq!(collect_indices(&out2), vec![0, 1]);
    }

    #[test]
    fn anti_excludes_left_duplicates_that_match() {
        let left = rb_from_opt_i32("key", &[Some(1), Some(1), Some(2), Some(3), Some(3)]);
        let right = rb_from_opt_i32("key", &[Some(1), Some(3)]);
        let out = merge_anti_join(&left, &right).unwrap();
        assert_eq!(collect_indices(&out), vec![2]);
    }

    #[test]
    fn anti_skips_large_right_duplicate_run() {
        let left = rb_from_opt_i32("key", &[Some(1), Some(2), Some(3)]);

        let mut right_vals: Vec<Option<i32>> = vec![Some(1); 1024];
        right_vals.push(Some(2));
        let right = rb_from_opt_i32("key", &right_vals);

        let out = merge_anti_join(&left, &right).unwrap();
        // 1 and 2 match, so only 3 remains.
        assert_eq!(collect_indices(&out), vec![2]);
    }

    #[test]
    fn anti_all_null_dtype_short_circuit() {
        // If any join key column is the Null dtype, all left rows are anti-joined
        let left_col = Series::full_null("key", &DataType::Null, 3);
        let left = RecordBatch::from_nonempty_columns(vec![left_col]).unwrap();
        let right = rb_from_opt_i32("key", &[Some(1), Some(2)]);
        let out = merge_anti_join(&left, &right).unwrap();
        assert_eq!(collect_indices(&out), vec![0, 1, 2]);
    }

    #[test]
    fn anti_includes_both_null_comparisons() {
        // Inputs must be sorted ascending with nulls last (as sort_merge_join provides)
        let left = rb_from_opt_i32("key", &[Some(1), Some(2), None, None]);
        let right = rb_from_opt_i32("key", &[Some(2), None]);
        let out = merge_anti_join(&left, &right).unwrap();
        // Anti-join should include rows that don't have equality matches, including rows where comparator returns None
        // Expect indices: 0 (value 1), 2 and 3 (nulls)
        assert_eq!(collect_indices(&out), vec![0, 2, 3]);
    }
}
