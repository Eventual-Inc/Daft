use std::collections::HashSet;

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use crate::array::ops::as_arrow::AsArrow;
use crate::array::ops::DaftApproxCountDistinctAggable;
use crate::array::DataArray;
use crate::datatypes::{Field, UInt64Array};
use crate::utils::identity_hash_set::IdentityBuildHasher;
use crate::DataType;

impl DaftApproxCountDistinctAggable for UInt64Array {
    type Output = DaftResult<UInt64Array>;

    fn approx_count_distinct(&self) -> Self::Output {
        let mut set = HashSet::<_, IdentityBuildHasher>::default();
        for &value in self.as_arrow().values_iter() {
            set.insert(value);
        }
        let count = set.len() as u64;
        let field = Field::new(self.name(), DataType::UInt64);
        let data = PrimitiveArray::from_vec(vec![count]).boxed();
        DataArray::new(field.into(), data)
    }

    fn grouped_approx_count_distinct(&self, groups: &super::GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let hll_iter = groups.iter().map(|group| {
            let mut set = HashSet::<_, IdentityBuildHasher>::default();
            for &index in group {
                if let Some(value) = data.get(index as _) {
                    set.insert(value);
                }
            }
            set.len() as u64
        });
        let field = Field::new(self.name(), DataType::UInt64);
        let data = PrimitiveArray::from_trusted_len_values_iter(hll_iter).boxed();
        DataArray::new(field.into(), data)
    }
}
