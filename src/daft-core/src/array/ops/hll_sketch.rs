use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{FixedSizeBinaryArray, UInt64Array},
    DataType,
};
use hyperloglog::{HyperLogLog, NUM_REGISTERS};

use common_error::DaftResult;

use crate::array::ops::{DaftHllSketchAggable, GroupIndices};

pub const HLL_SKETCH_DTYPE: DataType = DataType::FixedSizeBinary(NUM_REGISTERS);

impl DaftHllSketchAggable for UInt64Array {
    type Output = DaftResult<FixedSizeBinaryArray>;

    fn hll_sketch(&self) -> Self::Output {
        let mut hll = HyperLogLog::default();
        match self.validity() {
            Some(validity) => {
                for (index, &value) in self.as_arrow().values_iter().enumerate() {
                    if validity.get_bit(index) {
                        hll.add_already_hashed(value);
                    };
                }
            }
            None => {
                for &value in self.as_arrow().values_iter() {
                    hll.add_already_hashed(value);
                }
            }
        }
        let array = (self.name(), hll.registers.as_ref() as &[u8]).into();
        Ok(array)
    }

    fn grouped_hll_sketch(&self, group_indices: &GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut bytes = Vec::<u8>::with_capacity(group_indices.len() * NUM_REGISTERS);
        match self.validity() {
            Some(validity) => {
                for group in group_indices {
                    let mut hll = HyperLogLog::default();
                    for &index in group {
                        if let (Some(value), true) =
                            (data.get(index as _), validity.get_bit(index as _))
                        {
                            hll.add_already_hashed(value);
                        };
                    }
                    bytes.extend(hll.registers.as_ref());
                }
            }
            None => {
                for group in group_indices {
                    let mut hll = HyperLogLog::default();
                    for &index in group {
                        if let Some(value) = data.get(index as _) {
                            hll.add_already_hashed(value);
                        };
                    }
                    bytes.extend(hll.registers.as_ref());
                }
            }
        };
        let array = (self.name(), bytes.as_slice()).into();
        Ok(array)
    }
}
