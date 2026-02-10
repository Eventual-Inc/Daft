use arrow::array::Array;
use common_error::DaftResult;
use hyperloglog::HyperLogLog;

use crate::{
    array::ops::{DaftHllMergeAggable, GroupIndices, as_arrow::AsArrow},
    datatypes::{FixedSizeBinaryArray, UInt64Array},
};

impl DaftHllMergeAggable for FixedSizeBinaryArray {
    type Output = DaftResult<UInt64Array>;

    fn hll_merge(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;
        let mut final_hll = HyperLogLog::default();
        for byte_slice in arrow_array.iter().flatten() {
            let hll = HyperLogLog::new_with_byte_slice(byte_slice);
            final_hll.merge(&hll);
        }
        let count = final_hll.count() as u64;
        let array = UInt64Array::from_slice(self.name(), &[count]);
        Ok(array)
    }

    fn grouped_hll_merge(&self, groups: &GroupIndices) -> Self::Output {
        let data = self.as_arrow()?;
        let mut counts = Vec::with_capacity(groups.len());
        for group in groups {
            let mut final_hll = HyperLogLog::default();
            for &index in group {
                if data.is_valid(index as usize) {
                    let byte_slice = data.value(index as usize);
                    let hll = HyperLogLog::new_with_byte_slice(byte_slice);
                    final_hll.merge(&hll);
                }
            }
            let count = final_hll.count() as u64;
            counts.push(count);
        }
        let array = UInt64Array::from_slice(self.name(), &counts);
        Ok(array)
    }
}
