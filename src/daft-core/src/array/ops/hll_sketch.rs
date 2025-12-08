use common_error::DaftResult;
use daft_arrow::array::Array;
use hyperloglog::{HyperLogLog, NUM_REGISTERS};

use crate::{
    array::ops::{DaftHllSketchAggable, GroupIndices, as_arrow::AsArrow},
    datatypes::{DataType, FixedSizeBinaryArray, UInt64Array},
};

pub const HLL_SKETCH_DTYPE: DataType = DataType::FixedSizeBinary(NUM_REGISTERS);

impl DaftHllSketchAggable for UInt64Array {
    type Output = DaftResult<FixedSizeBinaryArray>;

    fn hll_sketch(&self) -> Self::Output {
        let mut hll = HyperLogLog::default();
        for value in self.as_arrow().iter().flatten() {
            hll.add_already_hashed(value);
        }
        let array = (self.name(), hll.registers.as_ref(), NUM_REGISTERS).into();
        Ok(array)
    }

    fn grouped_hll_sketch(&self, group_indices: &GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut bytes = Vec::<u8>::with_capacity(group_indices.len() * NUM_REGISTERS);
        for group in group_indices {
            let mut hll = HyperLogLog::default();
            for &index in group {
                if data.is_valid(index as _) {
                    hll.add_already_hashed(data.value(index as _));
                }
            }
            bytes.extend(hll.registers.as_ref());
        }
        let array = (self.name(), bytes, NUM_REGISTERS).into();
        Ok(array)
    }
}
