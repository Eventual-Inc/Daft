use std::{hash::RandomState, sync::Arc};

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};

use crate::{
    array::{
        ops::{as_arrow::AsArrow, DaftHllAggable, GroupIndices},
        DataArray,
    },
    datatypes::{DaftPhysicalType, Field, UInt64Type},
    DataType,
};

const PRECISION: u8 = 16;

impl<T> DaftHllAggable for &DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<UInt64Type>>;

    fn hll(&self) -> Self::Output {
        let mut hll_plus =
            HyperLogLogPlus::<u64, _>::new(PRECISION, RandomState::default()).unwrap();
        for value in self
            .as_any()
            .downcast_ref::<DataArray<UInt64Type>>()
            .unwrap()
            .as_arrow()
            .values_iter()
        {
            hll_plus.insert(value);
        }
        let result = hll_plus.count().trunc() as u64;
        let field = Field::new(self.name(), DataType::Float64);
        let field = Arc::new(field);
        let data = Box::new(PrimitiveArray::from([Some(result)]));
        DataArray::new(field, data)
    }

    fn grouped_hll(&self, _: &GroupIndices) -> Self::Output {
        todo!()
    }
}
