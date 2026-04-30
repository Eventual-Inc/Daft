use std::sync::Arc;

use arrow::{array::Array, buffer::Buffer};
use daft_common::error::DaftResult;
use daft_hash::hyperloglog::{HyperLogLog, NUM_REGISTERS};

use crate::{
    array::ops::{DaftHllMergeAggable, GroupIndices, as_arrow::AsArrow},
    datatypes::{DataType, Field, FixedSizeBinaryArray},
};

impl DaftHllMergeAggable for FixedSizeBinaryArray {
    type Output = DaftResult<Self>;

    fn hll_merge(&self) -> Self::Output {
        let arrow_array = self.as_arrow()?;
        let mut final_hll = HyperLogLog::default();
        for byte_slice in arrow_array.iter().flatten() {
            let hll = HyperLogLog::new_with_byte_slice(byte_slice);
            final_hll.merge(&hll);
        }
        Self::from_arrow(
            Field::new(self.name(), DataType::FixedSizeBinary(NUM_REGISTERS)),
            Arc::new(arrow::array::FixedSizeBinaryArray::new(
                NUM_REGISTERS as _,
                Buffer::from(final_hll.registers.to_vec()),
                None,
            )),
        )
    }

    fn grouped_hll_merge(&self, groups: &GroupIndices) -> Self::Output {
        let data = self.as_arrow()?;
        let mut bytes = Vec::<u8>::with_capacity(groups.len() * NUM_REGISTERS);
        for group in groups {
            let mut final_hll = HyperLogLog::default();
            for &index in group {
                if data.is_valid(index as usize) {
                    let byte_slice = data.value(index as usize);
                    let hll = HyperLogLog::new_with_byte_slice(byte_slice);
                    final_hll.merge(&hll);
                }
            }
            bytes.extend(final_hll.registers.as_ref());
        }
        Self::from_arrow(
            Field::new(self.name(), DataType::FixedSizeBinary(NUM_REGISTERS)),
            Arc::new(arrow::array::FixedSizeBinaryArray::new(
                NUM_REGISTERS as _,
                Buffer::from(bytes),
                None,
            )),
        )
    }
}
