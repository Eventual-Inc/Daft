use std::collections::HashSet;

use common_error::DaftResult;

use crate::{
    array::ops::DaftApproxCountDistinctAggable, datatypes::UInt64Array,
    utils::identity_hash_set::IdentityBuildHasher,
};

impl DaftApproxCountDistinctAggable for UInt64Array {
    type Output = DaftResult<Self>;

    fn approx_count_distinct(&self) -> Self::Output {
        let mut set = HashSet::with_capacity_and_hasher(self.len(), IdentityBuildHasher::default());
        for value in self.iter().flatten() {
            set.insert(value);
        }
        let count = set.len() as u64;
        let array = Self::from_slice(self.name(), &[count]);
        Ok(array)
    }

    fn grouped_approx_count_distinct(&self, groups: &super::GroupIndices) -> Self::Output {
        let data = self.values();
        let count_iter = groups.iter().map(|group| {
            let mut set = HashSet::<_, IdentityBuildHasher>::with_capacity_and_hasher(
                group.len(),
                IdentityBuildHasher::default(),
            );
            for &index in group {
                if let Some(value) = data.get(index as usize) {
                    set.insert(value);
                }
            }
            set.len() as u64
        });
        Ok(Self::from_values(self.name(), count_iter))
    }
}
