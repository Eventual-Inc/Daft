use std::sync::Arc;

use crate::utils::hyperloglog::{HyperLogLog, NUM_REGISTERS};
use arrow2::{array::PrimitiveArray, buffer::Buffer};
use common_error::DaftResult;

use crate::{
    array::{
        ops::{as_arrow::AsArrow, DaftHllAggable, GroupIndices},
        DataArray,
    },
    datatypes::{DaftPhysicalType, Field, UInt64Type},
    DataType,
};

impl<T> DaftHllAggable for &DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<UInt64Type>>;

    fn hll(&self) -> Self::Output {
        let mut hll = HyperLogLog::default();
        for &value in self
            .as_any()
            .downcast_ref::<DataArray<UInt64Type>>()
            .unwrap()
            .as_arrow()
            .values_iter()
        {
            hll.add_already_hashed(value);
        }
        let registers = &hll.registers;
        let dtype = DataType::FixedSizeBinary(NUM_REGISTERS);
        let values = Buffer::from(registers.to_vec());
        let data = Box::new(PrimitiveArray::new(dtype.to_arrow()?, values, None));
        let field = Arc::new(Field::new(self.name(), dtype));
        DataArray::new(field, data)
    }

    fn grouped_hll(&self, _: &GroupIndices) -> Self::Output {
        todo!()
    }
}
