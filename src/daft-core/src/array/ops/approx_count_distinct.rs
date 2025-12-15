use std::collections::HashSet;

use common_error::DaftResult;
use daft_arrow::array::PrimitiveArray;

use crate::{
    array::ops::{DaftApproxCountDistinctAggable, as_arrow::AsArrow},
    datatypes::UInt64Array,
    utils::identity_hash_set::IdentityBuildHasher,
};

impl DaftApproxCountDistinctAggable for UInt64Array {
    type Output = DaftResult<Self>;

    fn approx_count_distinct(&self) -> Self::Output {
        let mut set = HashSet::with_capacity_and_hasher(self.len(), IdentityBuildHasher::default());
        for &value in self.as_arrow2().iter().flatten() {
            set.insert(value);
        }
        let count = set.len() as u64;
        let data: &[u64] = &[count];
        let array = (self.name(), data).into();
        Ok(array)
    }

    fn grouped_approx_count_distinct(&self, groups: &super::GroupIndices) -> Self::Output {
        let data = self.as_arrow2();
        let count_iter = groups.iter().map(|group| {
            let mut set = HashSet::<_, IdentityBuildHasher>::with_capacity_and_hasher(
                group.len(),
                IdentityBuildHasher::default(),
            );
            for &index in group {
                if let Some(value) = data.get(index as _) {
                    set.insert(value);
                }
            }
            set.len() as u64
        });
        let data = Box::new(PrimitiveArray::from_trusted_len_values_iter(count_iter));
        let array = (self.name(), data).into();
        Ok(array)
    }
}
