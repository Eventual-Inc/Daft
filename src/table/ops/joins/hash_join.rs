use std::hash::{Hasher, BuildHasherDefault};

use crate::{
    array::{ops::arrow2::comparison::build_multi_array_is_equal, BaseArray},
    datatypes::{DataType, UInt64Array},
    error::{DaftError, DaftResult},
    series::Series,
    table::Table,
};

use crate::array::ops::downcast::Downcastable;

#[derive(Default)]
pub struct IdentityHasher {
    hash: u64,
}

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("IdentityHasher should be used by u64")
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
}

pub type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

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
            Series::empty("left_indices", &DataType::UInt64)?,
            Series::empty("right_indices", &DataType::UInt64)?,
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
    for (i, h) in r_hashes.downcast().values_iter().enumerate() {
        if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
            *h == other.hash && {
                let j = other.idx;
                is_equal(j as usize, i)
            }
        }) {
            for j in indices {
                left_idx.push(*j);
                right_idx.push(i as u64);
            }
        }
    }
    let left_series = UInt64Array::from(("left_indices", left_idx));
    let right_series = UInt64Array::from(("right_indices", right_idx));
    Ok((left_series.into_series(), right_series.into_series()))
}
