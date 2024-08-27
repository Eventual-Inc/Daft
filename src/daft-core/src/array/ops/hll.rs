use std::sync::Arc;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{Field, FixedSizeBinaryArray, UInt64Array},
    utils::hyperloglog::{HyperLogLog, NUM_REGISTERS},
    DataType,
};
use arrow2::{array::FixedSizeBinaryArray as Arrow2FixedSizeBinaryArray, buffer::Buffer};
use common_error::DaftResult;

use crate::array::{
    ops::{DaftHllAggable, GroupIndices},
    DataArray,
};

fn construct_field(name: &str) -> Arc<Field> {
    Arc::new(Field::new(name, DataType::FixedSizeBinary(NUM_REGISTERS)))
}

fn construct_data(bytes: Vec<u8>) -> Box<Arrow2FixedSizeBinaryArray> {
    Box::new(Arrow2FixedSizeBinaryArray::new(
        DataType::FixedSizeBinary(NUM_REGISTERS).to_arrow().unwrap(),
        Buffer::from(bytes),
        None,
    ))
}

impl DaftHllAggable for UInt64Array {
    type Output = DaftResult<FixedSizeBinaryArray>;

    fn hll(&self) -> Self::Output {
        let mut hll = HyperLogLog::default();
        for &value in self.as_arrow().values_iter() {
            hll.add_already_hashed(value);
        }
        let field = construct_field(self.name());
        let data = construct_data(hll.registers.to_vec());
        DataArray::new(field, data)
    }

    fn grouped_hll(&self, group_indices: &GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut bytes = Vec::with_capacity(group_indices.len() * NUM_REGISTERS);
        for group in group_indices {
            let mut hll = HyperLogLog::default();
            for &index in group {
                if let Some(value) = data.get(index as _) {
                    hll.add_already_hashed(value);
                };
            }
            bytes.extend(hll.registers);
        }
        let field = construct_field(self.name());
        let data = construct_data(bytes);
        DataArray::new(field, data)
    }
}
