use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, full::FullNull},
    datatypes::{DataType, UInt64Array},
    series::{IntoSeries, Series},
};

use crate::Table;
use common_error::{DaftError, DaftResult};

use daft_core::array::ops::as_arrow::AsArrow;

pub(super) fn hash_inner_join(left: &Table, right: &Table) -> DaftResult<(Series, Series)> {
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

    let probe_table = left.to_probe_hash_table()?;

    let r_hashes = right.hash_rows()?;
    let mut left_idx = vec![];
    let mut right_idx = vec![];
    let is_equal = build_multi_array_is_equal(
        left.columns.as_slice(),
        right.columns.as_slice(),
        false,
        false,
    )?;
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
    let left_series = UInt64Array::from(("left_indices", left_idx));
    let right_series = UInt64Array::from(("right_indices", right_idx));
    Ok((left_series.into_series(), right_series.into_series()))
}
