use std::sync::Arc;

use arrow::{array::Array, buffer::Buffer};
use common_error::DaftResult;
use hyperloglog::{HyperLogLog, NUM_REGISTERS};

use crate::{
    array::ops::{DaftHllSketchAggable, GroupIndices, as_arrow::AsArrow},
    datatypes::{DataType, Field, FixedSizeBinaryArray, UInt64Array},
};

pub const HLL_SKETCH_DTYPE: DataType = DataType::FixedSizeBinary(NUM_REGISTERS);

impl DaftHllSketchAggable for UInt64Array {
    type Output = DaftResult<FixedSizeBinaryArray>;

    fn hll_sketch(&self) -> Self::Output {
        let mut hll = HyperLogLog::default();
        for value in self.as_arrow()?.iter().flatten() {
            hll.add_already_hashed(value);
        }
        FixedSizeBinaryArray::from_arrow(
            Field::new(self.name(), DataType::FixedSizeBinary(NUM_REGISTERS)),
            Arc::new(arrow::array::FixedSizeBinaryArray::new(
                NUM_REGISTERS as _,
                Buffer::from(hll.registers.to_vec()),
                None,
            )),
        )
    }

    fn grouped_hll_sketch(&self, group_indices: &GroupIndices) -> Self::Output {
        let data = self.as_arrow()?;
        let mut bytes = Vec::<u8>::with_capacity(group_indices.len() * NUM_REGISTERS);
        for group in group_indices {
            let mut hll = HyperLogLog::default();
            for &index in group {
                let i = index as usize;
                if !data.is_null(i) {
                    hll.add_already_hashed(data.value(i));
                }
            }
            bytes.extend(hll.registers.as_ref());
        }
        FixedSizeBinaryArray::from_arrow(
            Field::new(self.name(), DataType::FixedSizeBinary(NUM_REGISTERS)),
            Arc::new(arrow::array::FixedSizeBinaryArray::new(
                NUM_REGISTERS as _,
                Buffer::from(bytes),
                None,
            )),
        )
    }
}
