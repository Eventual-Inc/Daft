use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use crate::{
    array::ops::{as_arrow::AsArrow, DaftHllMergeAggable},
    datatypes::{Field, FixedSizeBinaryArray, UInt64Array},
    utils::hyperloglog::HyperLogLog,
    DataType,
};

use crate::array::ops::GroupIndices;

impl DaftHllMergeAggable for &FixedSizeBinaryArray {
    type Output = DaftResult<UInt64Array>;

    fn hll_merge(&self) -> Self::Output {
        let mut final_hll = HyperLogLog::default();
        for byte_slice in self.as_arrow().values_iter() {
            let hll = HyperLogLog::new_with_byte_slice(byte_slice);
            final_hll.merge(&hll);
        }
        let count = final_hll.count() as u64;
        let field = Field::new(self.name(), DataType::UInt64).into();
        let data = PrimitiveArray::from([Some(count)]).boxed();
        UInt64Array::new(field, data)
    }

    fn grouped_hll_merge(&self, groups: &GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut counts = vec![];
        for group in groups {
            let mut final_hll = HyperLogLog::default();
            for &index in group {
                if let Some(byte_slice) = data.get(index as _) {
                    let hll = HyperLogLog::new_with_byte_slice(byte_slice);
                    final_hll.merge(&hll);
                }
            }
            let count = final_hll.count() as u64;
            counts.push(Some(count));
        }
        let field = Field::new(self.name(), DataType::UInt64).into();
        let data = PrimitiveArray::from(counts).boxed();
        UInt64Array::new(field, data)
    }
}
