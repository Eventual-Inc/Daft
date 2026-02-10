use std::sync::Arc;

use arrow::array::{Array, UInt64Builder};
use common_error::DaftResult;
use hyperloglog::HyperLogLog;

use crate::{
    array::ops::{DaftHllMergeAggable, GroupIndices, as_arrow::AsArrow},
    datatypes::{Field, FixedSizeBinaryArray, UInt64Array},
    prelude::DataType,
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
        let mut builder = UInt64Builder::with_capacity(1);
        builder.append_value(count);
        UInt64Array::from_arrow(
            Field::new(self.name(), DataType::UInt64),
            Arc::new(builder.finish()),
        )
    }

    fn grouped_hll_merge(&self, groups: &GroupIndices) -> Self::Output {
        let data = self.as_arrow()?;
        let mut builder = UInt64Builder::with_capacity(groups.len());
        for group in groups {
            let mut final_hll = HyperLogLog::default();
            for &index in group {
                if data.is_valid(index as usize) {
                    let byte_slice = data.value(index as usize);
                    let hll = HyperLogLog::new_with_byte_slice(byte_slice);
                    final_hll.merge(&hll);
                }
            }
            builder.append_value(final_hll.count() as u64);
        }
        UInt64Array::from_arrow(
            Field::new(self.name(), DataType::UInt64),
            Arc::new(builder.finish()),
        )
    }
}
