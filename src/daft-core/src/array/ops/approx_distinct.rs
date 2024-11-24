use std::collections::HashSet;

use common_error::DaftResult;

use crate::{
    array::{
        ops::{as_arrow::AsArrow, DaftApproxDistinctAggable},
        ListArray,
    },
    datatypes::UInt64Array,
    series::IntoSeries,
    utils::identity_hash_set::IdentityBuildHasher,
};

impl DaftApproxDistinctAggable for UInt64Array {
    type Output = DaftResult<ListArray>;

    fn approx_distinct(&self) -> Self::Output {
        let mut set = HashSet::with_capacity_and_hasher(self.len(), IdentityBuildHasher::default());
        let mut index_vec: Vec<u64> = Vec::new();
        for (i, &value) in self.as_arrow().iter().flatten().enumerate() {
            if set.insert(value) {
                index_vec.push(i as u64);
            }
        }
        let child_series = Self::from((self.name(), index_vec)).into_series();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(vec![0, child_series.len() as i64])?;
        let list_field = self.field.to_list_field()?;
        Ok(ListArray::new(list_field, child_series, offsets, None))
    }

    fn grouped_approx_distinct(&self, groups: &super::GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut offsets = vec![0i64];
        let mut all_indices = Vec::new();

        for group in groups {
            let mut set = HashSet::<_, IdentityBuildHasher>::with_capacity_and_hasher(
                group.len(),
                IdentityBuildHasher::default(),
            );
            for &index in group {
                if let Some(value) = data.get(index as _) {
                    if set.insert(value) {
                        all_indices.push(index);
                    }
                }
            }
            offsets.push(offsets.last().unwrap() + set.len() as i64);
        }

        let child_series = Self::from((self.name(), all_indices)).into_series();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let list_field = self.field.to_list_field()?;
        Ok(ListArray::new(list_field, child_series, offsets, None))
    }
}
