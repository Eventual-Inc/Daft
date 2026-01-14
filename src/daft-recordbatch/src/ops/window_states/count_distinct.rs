use std::collections::HashMap;

use arrow::{
    array::{DynComparator, make_comparator},
    compute::SortOptions,
};
use common_error::DaftResult;
use daft_core::{
    prelude::*,
    utils::identity_hash_set::{IdentityBuildHasher, IndexHash},
};

use super::WindowAggStateOps;

pub struct CountDistinctWindowState {
    hashed: DataArray<UInt64Type>,
    counts: HashMap<IndexHash, usize, IdentityBuildHasher>,
    count_vec: Vec<u64>,
    comparator: DynComparator,
}

impl CountDistinctWindowState {
    pub fn new(source: &Series, total_length: usize) -> Self {
        let hashed = source.hash_with_nulls(None).unwrap();

        #[allow(deprecated, reason = "arrow2 migration")]
        let array = source.to_arrow().unwrap();

        let comparator =
            make_comparator(array.as_ref(), array.as_ref(), SortOptions::default()).unwrap();

        Self {
            hashed,
            counts: HashMap::with_capacity_and_hasher(total_length, Default::default()),
            count_vec: Vec::with_capacity(total_length),
            comparator,
        }
    }
}

impl WindowAggStateOps for CountDistinctWindowState {
    fn add(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        for i in start_idx..end_idx {
            if let Some(hash) = self.hashed.get(i) {
                let index_hash = IndexHash {
                    idx: i as u64,
                    hash,
                };

                let mut found_match = false;
                for (existing_hash, count) in &mut self.counts {
                    if existing_hash.hash == hash
                        && (self.comparator)(i, existing_hash.idx as usize).is_eq()
                    {
                        *count += 1;
                        found_match = true;
                        break;
                    }
                }

                if !found_match {
                    self.counts.insert(index_hash, 1);
                }
            }
        }
        Ok(())
    }

    fn remove(&mut self, start_idx: usize, end_idx: usize) -> DaftResult<()> {
        assert!(
            end_idx > start_idx,
            "end_idx must be greater than start_idx"
        );

        for i in start_idx..end_idx {
            if let Some(hash) = self.hashed.get(i) {
                let mut keys_to_remove = Vec::new();

                for (k, v) in &mut self.counts {
                    if k.hash == hash && (self.comparator)(i, k.idx as usize).is_eq() {
                        *v -= 1;
                        if *v == 0 {
                            keys_to_remove.push(IndexHash {
                                idx: k.idx,
                                hash: k.hash,
                            });
                        }
                        break;
                    }
                }

                for key in keys_to_remove {
                    self.counts.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DaftResult<()> {
        self.count_vec.push(self.counts.len() as u64);
        Ok(())
    }

    fn build(&self) -> DaftResult<Series> {
        Ok(DataArray::<UInt64Type>::from(("", self.count_vec.clone())).into_series())
    }
}
