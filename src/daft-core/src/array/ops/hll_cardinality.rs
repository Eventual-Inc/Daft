use std::sync::Arc;

use arrow::array::{Array, UInt64Builder};
use daft_common::error::DaftResult;
use daft_hash::hyperloglog::HyperLogLog;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{Field, FixedSizeBinaryArray, UInt64Array},
    prelude::DataType,
};

impl FixedSizeBinaryArray {
    pub fn hll_cardinality(&self) -> DaftResult<UInt64Array> {
        let arrow_array = self.as_arrow()?;
        let mut builder = UInt64Builder::with_capacity(arrow_array.len());
        for opt_bytes in arrow_array {
            match opt_bytes {
                Some(byte_slice) => {
                    let hll = HyperLogLog::new_with_byte_slice(byte_slice);
                    builder.append_value(hll.count() as u64);
                }
                None => builder.append_null(),
            }
        }
        UInt64Array::from_arrow(
            Field::new(self.name(), DataType::UInt64),
            Arc::new(builder.finish()),
        )
    }
}
