use std::cmp::Ordering;

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::full::FullNull,
    datatypes::{DataType, Field, UInt64Array},
    kernels::search_sorted::build_partial_compare_with_nulls,
};

use crate::RecordBatch;

pub fn asof_join_backward(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    if left.num_columns() != right.num_columns() {
        return Err(DaftError::ValueError(format!(
            "Mismatch of asof key columns: left: {:?} vs right: {:?}",
            left.num_columns(),
            right.num_columns()
        )));
    }
    if left.num_columns() == 0 {
        return Err(DaftError::ValueError(
            "No columns were passed in to asof join on".to_string(),
        ));
    }

    let types_not_match = left
        .columns
        .iter()
        .zip(right.columns.iter())
        .any(|(l, r)| l.data_type() != r.data_type());
    if types_not_match {
        return Err(DaftError::SchemaMismatch(
            "Types between left and right asof keys do not match".to_string(),
        ));
    }

    if left.is_empty() {
        return Ok((
            UInt64Array::empty("left_indices", &DataType::UInt64),
            UInt64Array::empty("right_indices", &DataType::UInt64),
        ));
    }
    if right.is_empty() {
        return Ok((
            UInt64Array::from_vec("left_indices", (0..left.len() as u64).collect()),
            UInt64Array::full_null("right_indices", &DataType::UInt64, left.len()),
        ));
    }

    let num_by_keys = left.num_columns() - 1;
    let left_on = left.get_column(num_by_keys);
    let right_on = right.get_column(num_by_keys);

    // Comparator for "on" column.
    let on_cmp = build_partial_compare_with_nulls(
        left_on.to_arrow()?.as_ref(),
        right_on.to_arrow()?.as_ref(),
        false,
    )?;

    // Individual column comparators for "by" columns.
    let by_cmp_list = (0..num_by_keys)
        .map(|idx| {
            build_partial_compare_with_nulls(
                left.get_column(idx).to_arrow()?.as_ref(),
                right.get_column(idx).to_arrow()?.as_ref(),
                false,
            )
        })
        .collect::<DaftResult<Vec<_>>>()?;

    // Combined comparator for "by" columns.
    let cmp_by = |left_idx: usize, right_idx: usize| -> Option<Ordering> {
        for comparator in &by_cmp_list {
            match comparator(left_idx, right_idx) {
                Some(Ordering::Equal) => {}
                other => return other,
            }
        }
        Some(Ordering::Equal)
    };

    // Individual column comparators for "by" columns in left table.
    let left_by_cmp_list = (0..num_by_keys)
        .map(|idx| {
            build_partial_compare_with_nulls(
                left.get_column(idx).to_arrow()?.as_ref(),
                left.get_column(idx).to_arrow()?.as_ref(),
                false,
            )
        })
        .collect::<DaftResult<Vec<_>>>()?;

    // Combined equality check on "by" columns in left table.
    let same_left_group = |prev_left_idx: usize, left_idx: usize| -> bool {
        for comparator in &left_by_cmp_list {
            if !matches!(comparator(prev_left_idx, left_idx), Some(Ordering::Equal)) {
                return false;
            }
        }
        true
    };

    let mut right_idx = 0usize;
    let mut best_match: Option<u64> = None;
    let mut right_indices = Vec::with_capacity(left.len());
    for left_idx in 0..left.len() {
        if left_idx > 0 && num_by_keys > 0 && !same_left_group(left_idx - 1, left_idx) {
            best_match = None;
        }

        while right_idx < right.len() {
            match cmp_by(left_idx, right_idx) {
                Some(Ordering::Greater) | None => {
                    right_idx += 1;
                    best_match = None;
                }
                Some(Ordering::Equal) | Some(Ordering::Less) => break,
            }
        }

        if right_idx == right.len() || !matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
        {
            right_indices.push(best_match);
            continue;
        }

        while right_idx < right.len()
            && matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
        {
            match on_cmp(left_idx, right_idx) {
                Some(Ordering::Greater) | Some(Ordering::Equal) => {
                    best_match = Some(right_idx as u64);
                    right_idx += 1;
                }
                Some(Ordering::Less) | None => break,
            }
        }
        right_indices.push(best_match);
    }

    let left_indices = UInt64Array::from_vec("left_indices", (0..left.len() as u64).collect());
    let right_indices = UInt64Array::from_iter(
        Field::new("right_indices", DataType::UInt64),
        right_indices.into_iter(),
    );
    Ok((left_indices, right_indices))
}
