use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{VecIndices, arrow::comparison::build_multi_array_is_equal},
    datatypes::UInt64Array,
    series::Series,
    utils::identity_hash_set::{IdentityBuildHasher, IndexHash},
};
use hashbrown::{HashMap, hash_map::RawEntryMut};
use smallvec::smallvec;

use crate::RecordBatch;

impl RecordBatch {
    pub fn hash_rows(&self) -> DaftResult<UInt64Array> {
        if self.num_columns() == 0 {
            return Err(DaftError::ValueError(
                "Attempting to Hash Table with no columns".to_string(),
            ));
        }
        let cols = self.as_materialized_series();
        let mut hash_so_far = cols.first().unwrap().hash(None)?;
        for c in cols.iter().skip(1) {
            hash_so_far = c.hash(Some(&hash_so_far))?;
        }
        Ok(hash_so_far)
    }

    /// Like [`Self::hash_rows`] but mixes in a `seed`, producing a different hash for the same rows
    /// across different seeds. Used for recursive re-partitioning of an overflowing hash bucket
    /// (spilling grace aggregation): re-hashing with the *same* seed would map every row back into
    /// the same sub-bucket, so a per-level seed is required for the recursion to make progress.
    /// Equal keys under the same seed still hash identically (so groups stay co-located).
    pub fn hash_rows_seeded(&self, seed: u64) -> DaftResult<UInt64Array> {
        if self.num_columns() == 0 {
            return Err(DaftError::ValueError(
                "Attempting to Hash Table with no columns".to_string(),
            ));
        }
        let cols = self.as_materialized_series();
        let seed_arr = UInt64Array::from_vec("seed", vec![seed; self.len()]);
        let mut hash_so_far = cols.first().unwrap().hash(Some(&seed_arr))?;
        for c in cols.iter().skip(1) {
            hash_so_far = c.hash(Some(&hash_so_far))?;
        }
        Ok(hash_so_far)
    }

    pub fn to_probe_hash_table(
        &self,
    ) -> DaftResult<HashMap<IndexHash, VecIndices, IdentityBuildHasher>> {
        let hashes = self.hash_rows()?;

        const DEFAULT_SIZE: usize = 20;
        let cols: Vec<Series> = self.as_materialized_series().into_iter().cloned().collect();
        let comparator = build_multi_array_is_equal(
            cols.as_slice(),
            cols.as_slice(),
            vec![true; cols.len()].as_slice(),
            vec![true; cols.len()].as_slice(),
        )?;

        let mut probe_table =
            HashMap::<IndexHash, VecIndices, IdentityBuildHasher>::with_capacity_and_hasher(
                DEFAULT_SIZE,
                Default::default(),
            );
        // TODO(Sammy): Drop nulls using validity array if requested
        for (i, h) in hashes.values().iter().enumerate() {
            let entry = probe_table.raw_entry_mut().from_hash(*h, |other| {
                (*h == other.hash) && {
                    let j = other.idx;
                    comparator(i, j as usize)
                }
            });
            match entry {
                RawEntryMut::Vacant(entry) => {
                    entry.insert_hashed_nocheck(
                        *h,
                        IndexHash {
                            idx: i as u64,
                            hash: *h,
                        },
                        smallvec![i as u64],
                    );
                }
                RawEntryMut::Occupied(mut entry) => {
                    entry.get_mut().push(i as u64);
                }
            }
        }
        Ok(probe_table)
    }

    pub fn to_idx_hash_table(&self) -> DaftResult<HashMap<IndexHash, (), IdentityBuildHasher>> {
        let hashes = self.hash_rows()?;

        const DEFAULT_SIZE: usize = 20;
        let cols: Vec<Series> = self.as_materialized_series().into_iter().cloned().collect();
        let comparator = build_multi_array_is_equal(
            cols.as_slice(),
            cols.as_slice(),
            vec![true; cols.len()].as_slice(),
            vec![true; cols.len()].as_slice(),
        )?;

        let mut idx_hash_table =
            HashMap::<IndexHash, (), IdentityBuildHasher>::with_capacity_and_hasher(
                DEFAULT_SIZE,
                Default::default(),
            );
        // TODO(Sammy): Drop nulls using validity array if requested
        for (i, h) in hashes.values().iter().enumerate() {
            let entry = idx_hash_table.raw_entry_mut().from_hash(*h, |other| {
                (*h == other.hash) && {
                    let j = other.idx;
                    comparator(i, j as usize)
                }
            });
            match entry {
                RawEntryMut::Vacant(entry) => {
                    entry.insert_hashed_nocheck(
                        *h,
                        IndexHash {
                            idx: i as u64,
                            hash: *h,
                        },
                        (),
                    );
                }
                RawEntryMut::Occupied(_) => {}
            }
        }
        Ok(idx_hash_table)
    }

    pub fn to_probe_hash_map_without_idx(
        &self,
    ) -> DaftResult<HashMap<IndexHash, (), IdentityBuildHasher>> {
        let hashes = self.hash_rows()?;

        const DEFAULT_SIZE: usize = 20;
        let cols: Vec<Series> = self.as_materialized_series().into_iter().cloned().collect();
        let comparator = build_multi_array_is_equal(
            cols.as_slice(),
            cols.as_slice(),
            vec![true; cols.len()].as_slice(),
            vec![true; cols.len()].as_slice(),
        )?;

        let mut probe_table =
            HashMap::<IndexHash, (), IdentityBuildHasher>::with_capacity_and_hasher(
                DEFAULT_SIZE,
                Default::default(),
            );
        // TODO(Sammy): Drop nulls using validity array if requested
        for (i, h) in hashes.values().iter().enumerate() {
            let entry = probe_table.raw_entry_mut().from_hash(*h, |other| {
                (*h == other.hash) && {
                    let j = other.idx;
                    comparator(i, j as usize)
                }
            });
            match entry {
                RawEntryMut::Vacant(entry) => {
                    entry.insert_hashed_nocheck(
                        *h,
                        IndexHash {
                            idx: i as u64,
                            hash: *h,
                        },
                        (),
                    );
                }
                RawEntryMut::Occupied(_) => {}
            }
        }
        Ok(probe_table)
    }
}
