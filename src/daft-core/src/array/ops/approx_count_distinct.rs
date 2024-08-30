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
        match self.validity() {
            Some(validity) => {
                for (index, &value) in self.as_arrow().values_iter().enumerate() {
                    if validity.get_bit(index) {
                        set.insert(value);
                    };
                }
            }
            None => {
                for &value in self.as_arrow().values_iter() {
                    set.insert(value);
                }
            }
        };
        let count = set.len() as u64;
        let field = Field::new(self.name(), DataType::UInt64);
        let data = PrimitiveArray::from_vec(vec![count]).boxed();
        DataArray::new(field.into(), data)
    }

    fn grouped_approx_count_distinct(&self, groups: &super::GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let data = match self.validity() {
            Some(validity) => {
                let hll_iter = groups.iter().map(|group| {
                    let mut set = HashSet::<_, IdentityBuildHasher>::default();
                    for &index in group {
                        if let (Some(value), true) =
                            (data.get(index as _), validity.get_bit(index as _))
                        {
                            set.insert(value);
                        }
                    }
                    set.len() as u64
                });
                PrimitiveArray::from_trusted_len_values_iter(hll_iter).boxed()
            }
            None => {
                let hll_iter = groups.iter().map(|group| {
                    let mut set = HashSet::<_, IdentityBuildHasher>::default();
                    for &index in group {
                        if let Some(value) = data.get(index as _) {
                            set.insert(value);
                        }
                    }
                    set.len() as u64
                });
                PrimitiveArray::from_trusted_len_values_iter(hll_iter).boxed()
            }
        };
        let field = Field::new(self.name(), DataType::UInt64);
        DataArray::new(field.into(), data)
    }
}
