use std::{
    collections::{hash_map::Entry, HashMap},
    hash::{BuildHasherDefault, Hasher},
};

use crate::{
    array::{ops::arrow2::comparison::build_multi_array_is_equal, BaseArray},
    datatypes::{DataType, UInt64Array},
    error::{DaftError, DaftResult},
    series::Series,
    table::Table,
};

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
    let hashes = left.hash_rows()?;

    const DEFAULT_SIZE: usize = 20;

    let mut probe_table = HashMap::<u64, Vec<u64>, IdentityBuildHasher>::with_capacity_and_hasher(
        DEFAULT_SIZE,
        Default::default(),
    );

    for (i, h) in hashes.downcast().values_iter().enumerate() {
        let entry = probe_table.entry(*h);
        match entry {
            Entry::Vacant(entry) => {
                entry.insert(vec![i as u64]);
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(i as u64);
            }
        }
    }

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
        if let Some(indices) = probe_table.get(h) {
            for j in indices {
                if is_equal(*j as usize, i) {
                    left_idx.push(*j);
                    right_idx.push(i as u64);
                }
            }
        }
    }
    let left_series = UInt64Array::from(("left_indices", left_idx));
    let right_series = UInt64Array::from(("right_indices", right_idx));
    Ok((left_series.into_series(), right_series.into_series()))
}
