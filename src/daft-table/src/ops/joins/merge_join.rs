use std::cmp::Ordering;

use daft_core::{
    array::ops::full::FullNull,
    datatypes::{DataType, UInt64Array},
    kernels::search_sorted::build_compare_with_nulls,
    series::{IntoSeries, Series},
};

use crate::Table;
use common_error::{DaftError, DaftResult};

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

    // Construct comparator over all join keys.
    let mut cmp_list = Vec::with_capacity(left.num_columns());
    for (left_series, right_series) in left.columns.iter().zip(right.columns.iter()) {
        cmp_list.push(build_compare_with_nulls(
            left_series.to_arrow().as_ref(),
            right_series.to_arrow().as_ref(),
            false,
            Some(true),
            true,
        )?);
    }
    let combined_comparator = |a_idx: usize, b_idx: usize| -> Ordering {
        for comparator in cmp_list.iter() {
            match comparator(a_idx, b_idx) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    };

    // Perform the merge by building up left-side and right-side take index vectors.
    let mut left_indices = vec![];
    let mut right_indices = vec![];
    let mut left_idx = 0;
    let mut right_idx = 0;
    while left_idx < left.len() && right_idx < right.len() {
        let mut ord = combined_comparator(left_idx, right_idx);
        let mut left_lookahead_idx = left_idx;
        // While left and right matches, push index pairs.
        // We increment a left-side lookahead index, which we reset before the next outer loop in case
        // the next right-side element is equal to the current right-side element, in which case we'd need
        // to rematch all elements in this lookahead run.
        while matches!(ord, Ordering::Equal) {
            left_indices.push(left_lookahead_idx as u64);
            right_indices.push(right_idx as u64);
            left_lookahead_idx += 1;
            // Stop if we've exhausted the left table.
            if left_lookahead_idx >= left.len() {
                break;
            }
            ord = combined_comparator(left_lookahead_idx, right_idx);
        }
        match ord {
            Ordering::Less => {
                // Left element is less than right element, so need to move to next left element for potential match.
                // We move past the last lookahead index in case we matched a run of Equals followed by a left-side null, which
                // would evaluate as Less.
                left_idx = left_lookahead_idx + 1;
            }
            Ordering::Greater => {
                // Right element is less than left element, so need to move to next right element for potential match.
                right_idx += 1;
            }
            Ordering::Equal => {
                // Equality loop above must have broke with the lookahead reaching the end of the left table;
                // we must advance the right index.
                right_idx += 1;
            }
        }
    }
    let left_series = UInt64Array::from(("left_indices", left_indices));
    let right_series = UInt64Array::from(("right_indices", right_indices));
    Ok((left_series.into_series(), right_series.into_series()))
}
