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
        let mut values = Vec::with_capacity(self.len());
        for &value in self.as_arrow().iter().flatten() {
            if set.insert(value) {
                values.push(value);
            }
        }
        let child_series = Self::from((self.name(), values)).into_series();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(vec![0, child_series.len() as i64])?;
        let list_field = self.field.to_list_field()?;
        Ok(ListArray::new(list_field, child_series, offsets, None))
    }

    fn grouped_approx_distinct(&self, groups: &super::GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut offsets = Vec::with_capacity(groups.len() + 1);
        let mut values = Vec::new();
        offsets.push(0i64);

        for group in groups {
            let mut set = HashSet::<_, IdentityBuildHasher>::with_capacity_and_hasher(
                group.len(),
                IdentityBuildHasher::default(),
            );
            for &index in group {
                if let Some(value) = data.get(index as _) {
                    if set.insert(value) {
                        values.push(value);
                    }
                }
            }
            offsets.push(offsets.last().unwrap() + set.len() as i64);
        }

        let child_series = Self::from((self.name(), values)).into_series();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let list_field = self.field.to_list_field()?;
        Ok(ListArray::new(list_field, child_series, offsets, None))
    }
}
