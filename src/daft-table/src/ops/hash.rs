use std::{
    collections::{hash_map::RawEntryMut, HashMap},
    hash::{Hash, Hasher},
};

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, as_arrow::AsArrow},
    datatypes::UInt64Array,
    utils::identity_hash_set::IdentityBuildHasher,
};

use crate::Table;

pub struct IndexHash {
    pub idx: u64,
    pub hash: u64,
}

impl Hash for IndexHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl Table {
    pub fn hash_rows(&self) -> DaftResult<UInt64Array> {
        if self.num_columns() == 0 {
            return Err(DaftError::ValueError(
                "Attempting to Hash Table with no columns".to_string(),
            ));
        }
        let mut hash_so_far = self.columns.first().unwrap().hash(None)?;
        for c in self.columns.iter().skip(1) {
            hash_so_far = c.hash(Some(&hash_so_far))?;
        }
        Ok(hash_so_far)
    }

    pub fn to_probe_hash_table(
        &self,
    ) -> DaftResult<HashMap<IndexHash, Vec<u64>, IdentityBuildHasher>> {
        let hashes = self.hash_rows()?;

        const DEFAULT_SIZE: usize = 20;
        let comparator = build_multi_array_is_equal(
            self.columns.as_slice(),
            self.columns.as_slice(),
            true,
            true,
        )?;

        let mut probe_table =
            HashMap::<IndexHash, Vec<u64>, IdentityBuildHasher>::with_capacity_and_hasher(
                DEFAULT_SIZE,
                Default::default(),
            );
        // TODO(Sammy): Drop nulls using validity array if requested
        for (i, h) in hashes.as_arrow().values_iter().enumerate() {
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
                        vec![i as u64],
                    );
                }
                RawEntryMut::Occupied(mut entry) => {
                    entry.get_mut().push(i as u64);
                }
            }
        }
        Ok(probe_table)
    }

    pub fn to_probe_hash_map_without_idx(
        &self,
    ) -> DaftResult<HashMap<IndexHash, (), IdentityBuildHasher>> {
        let hashes = self.hash_rows()?;

        const DEFAULT_SIZE: usize = 20;
        let comparator = build_multi_array_is_equal(
            self.columns.as_slice(),
            self.columns.as_slice(),
            true,
            true,
        )?;

        let mut probe_table =
            HashMap::<IndexHash, (), IdentityBuildHasher>::with_capacity_and_hasher(
                DEFAULT_SIZE,
                Default::default(),
            );
        // TODO(Sammy): Drop nulls using validity array if requested
        for (i, h) in hashes.as_arrow().values_iter().enumerate() {
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
