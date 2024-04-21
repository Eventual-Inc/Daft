use std::iter::repeat;

use arrow2::types::IndexRange;
use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, full::FullNull},
    datatypes::UInt64Array,
    DataType, IntoSeries, JoinType, Series,
};

use crate::Table;
use common_error::{DaftError, DaftResult};

use daft_core::array::ops::as_arrow::AsArrow;

pub(super) fn hash_join(
    left: &Table,
    right: &Table,
    how: JoinType,
) -> DaftResult<(Series, Series)> {
    // TODO(sammy) add tests for mismatched types for multiple columns for joins
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
        let (left_arr, right_arr) = match how {
            JoinType::Inner => (
                UInt64Array::empty("left_indices", &DataType::UInt64),
                UInt64Array::empty("right_indices", &DataType::UInt64),
            ),
            JoinType::Left => (
                UInt64Array::from(("left_indices", (0..(left.len() as u64)).collect::<Vec<_>>())),
                UInt64Array::full_null("right_indices", &DataType::UInt64, left.len()),
            ),
            JoinType::Right => (
                UInt64Array::full_null("left_indices", &DataType::UInt64, right.len()),
                UInt64Array::from((
                    "right_indices",
                    (0..(right.len() as u64)).collect::<Vec<_>>(),
                )),
            ),
            JoinType::Outer => {
                let l_iter = IndexRange::new(0, left.len() as u64)
                    .map(Some)
                    .chain(repeat(None).take(right.len()));
                let r_iter = repeat(None)
                    .take(left.len())
                    .chain(IndexRange::new(0, right.len() as u64).map(Some));

                let l_arrow = Box::new(
                    arrow2::array::PrimitiveArray::<u64>::from_trusted_len_iter(l_iter),
                );
                let r_arrow = Box::new(
                    arrow2::array::PrimitiveArray::<u64>::from_trusted_len_iter(r_iter),
                );

                (
                    UInt64Array::from(("left_indices", l_arrow)),
                    UInt64Array::from(("right_indices", r_arrow)),
                )
            }
        };

        return Ok((left_arr.into_series(), right_arr.into_series()));
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

    let probe_left = match (how, left.len() <= right.len()) {
        (JoinType::Left, _) => false,
        (JoinType::Right, _) => true,
        (_, left_side_smaller) => left_side_smaller,
    };

    let (left, right) = if probe_left {
        (left, right)
    } else {
        (right, left)
    };

    let probe_table = left.to_probe_hash_table()?;

    let r_hashes = right.hash_rows()?;

    let is_equal = build_multi_array_is_equal(
        left.columns.as_slice(),
        right.columns.as_slice(),
        false,
        false,
    )?;

    let (lseries, rseries) = if how == JoinType::Inner {
        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for (r_idx, h) in r_hashes.as_arrow().values_iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let l_idx = other.idx;
                    is_equal(l_idx as usize, r_idx)
                }
            }) {
                for l_idx in indices {
                    left_idx.push(*l_idx);
                    right_idx.push(r_idx as u64);
                }
            }
        }

        (
            UInt64Array::from(("left_indices", left_idx)).into_series(),
            UInt64Array::from(("right_indices", right_idx)).into_series(),
        )
    } else {
        let mut left_idx = vec![];
        let mut right_idx = vec![];

        if how == JoinType::Outer {
            let mut left_idx_used = vec![false; left.len()];

            for (r_idx, h) in r_hashes.as_arrow().values_iter().enumerate() {
                if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                    *h == other.hash && {
                        let l_idx = other.idx;
                        is_equal(l_idx as usize, r_idx)
                    }
                }) {
                    for l_idx in indices {
                        left_idx.push(Some(*l_idx));
                        left_idx_used[*l_idx as usize] = true;

                        right_idx.push(Some(r_idx as u64));
                    }
                } else {
                    left_idx.push(None);
                    right_idx.push(Some(r_idx as u64));
                }
            }

            for (l_idx, used) in left_idx_used.iter().enumerate() {
                if !used {
                    left_idx.push(Some(l_idx as u64));
                    right_idx.push(None);
                }
            }
        } else {
            for (r_idx, h) in r_hashes.as_arrow().values_iter().enumerate() {
                if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                    *h == other.hash && {
                        let l_idx = other.idx;
                        is_equal(l_idx as usize, r_idx)
                    }
                }) {
                    for l_idx in indices {
                        left_idx.push(Some(*l_idx));
                        right_idx.push(Some(r_idx as u64));
                    }
                } else {
                    left_idx.push(None);
                    right_idx.push(Some(r_idx as u64));
                }
            }
        }

        let left_arrow = Box::new(arrow2::array::PrimitiveArray::<u64>::from_trusted_len_iter(
            left_idx.into_iter(),
        ));
        let right_arrow = Box::new(arrow2::array::PrimitiveArray::<u64>::from_trusted_len_iter(
            right_idx.into_iter(),
        ));

        (
            UInt64Array::from(("left_indices", left_arrow)).into_series(),
            UInt64Array::from(("right_indices", right_arrow)).into_series(),
        )
    };

    if probe_left {
        Ok((lseries, rseries))
    } else {
        Ok((rseries, lseries))
    }
}
