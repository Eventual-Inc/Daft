use std::sync::Arc;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{Field, FixedSizeBinaryArray, UInt64Array},
    DataType,
};
use hyperloglog::{HyperLogLog, NUM_REGISTERS};

use arrow2::{array::FixedSizeBinaryArray as Arrow2FixedSizeBinaryArray, buffer::Buffer};
use common_error::DaftResult;

use crate::array::{
    ops::{DaftHllSketchAggable, GroupIndices},
    DataArray,
};

pub const HLL_SKETCH_DTYPE: DataType = DataType::FixedSizeBinary(NUM_REGISTERS);

fn construct_field(name: &str) -> Arc<Field> {
    Arc::new(Field::new(name, HLL_SKETCH_DTYPE))
}

fn construct_data(bytes: Vec<u8>) -> Box<Arrow2FixedSizeBinaryArray> {
    Box::new(Arrow2FixedSizeBinaryArray::new(
        HLL_SKETCH_DTYPE.to_arrow().unwrap(),
        Buffer::from(bytes),
        None,
    ))
}

impl DaftHllSketchAggable for UInt64Array {
    type Output = DaftResult<FixedSizeBinaryArray>;

    fn hll_sketch(&self) -> Self::Output {
        let mut hll = HyperLogLog::default();
        for &value in self.as_arrow().values_iter() {
            hll.add_already_hashed(value);
        }
        let field = construct_field(self.name());
        let data = construct_data(hll.registers.to_vec());
        DataArray::new(field, data)
    }

    fn grouped_hll_sketch(&self, group_indices: &GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut bytes = Vec::with_capacity(group_indices.len() * NUM_REGISTERS);
        for group in group_indices {
            let mut hll = HyperLogLog::default();
            for &index in group {
                if let Some(value) = data.get(index as _) {
                    hll.add_already_hashed(value);
                };
            }
            bytes.extend(hll.registers.as_ref());
        }
        let field = construct_field(self.name());
        let data = construct_data(bytes);
        DataArray::new(field, data)
    }
}
