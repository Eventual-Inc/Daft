use std::sync::Arc;

use arrow2;

use crate::{
    array::{BaseArray, DataArray},
    datatypes::*,
    error::DaftResult,
};

use super::DaftCompareAggable;

impl<T> DaftCompareAggable for &DataArray<T>
where
    T: DaftDataType + DaftNumericType,
    <T::Native as arrow2::types::simd::Simd>::Simd: arrow2::compute::aggregate::SimdOrd<T::Native>,
{
    type Output = DaftResult<DataArray<T>>;

    fn min(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let arrow_array = match primitive_arr.len() {
            0 => arrow2::array::PrimitiveArray::from([]),
            _ => {
                let result = arrow2::compute::aggregate::min_primitive(primitive_arr);
                arrow2::array::PrimitiveArray::from([result])
            }
        };
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }

    fn max(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let arrow_array = match primitive_arr.len() {
            0 => arrow2::array::PrimitiveArray::from([]),
            _ => {
                let result = arrow2::compute::aggregate::max_primitive(primitive_arr);
                arrow2::array::PrimitiveArray::from([result])
            }
        };
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }
}

impl DaftCompareAggable for &DataArray<Utf8Type> {
    type Output = DaftResult<DataArray<Utf8Type>>;
    fn min(&self) -> Self::Output {
        let arrow_array: &arrow2::array::Utf8Array<i64> =
            self.data().as_any().downcast_ref().unwrap();

        let res_arrow_array = match arrow_array.len() {
            0 => arrow2::array::Utf8Array::<i64>::new_empty(arrow2::datatypes::DataType::LargeUtf8),
            _ => {
                let result = arrow2::compute::aggregate::min_string(arrow_array);
                arrow2::array::Utf8Array::<i64>::from([result])
            }
        };
        DataArray::new(self.field.clone(), Arc::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        let arrow_array: &arrow2::array::Utf8Array<i64> =
            self.data().as_any().downcast_ref().unwrap();

        let res_arrow_array = match arrow_array.len() {
            0 => arrow2::array::Utf8Array::<i64>::new_empty(arrow2::datatypes::DataType::LargeUtf8),
            _ => {
                let result = arrow2::compute::aggregate::max_string(arrow_array);
                arrow2::array::Utf8Array::<i64>::from([result])
            }
        };
        DataArray::new(self.field.clone(), Arc::new(res_arrow_array))
    }
}

impl DaftCompareAggable for &DataArray<BooleanType> {
    type Output = DaftResult<DataArray<BooleanType>>;
    fn min(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BooleanArray =
            self.data().as_any().downcast_ref().unwrap();

        let res_arrow_array = match arrow_array.len() {
            0 => arrow2::array::BooleanArray::new_empty(arrow2::datatypes::DataType::Boolean),
            _ => {
                let result = arrow2::compute::aggregate::min_boolean(arrow_array);
                arrow2::array::BooleanArray::from([result])
            }
        };
        DataArray::new(self.field.clone(), Arc::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BooleanArray =
            self.data().as_any().downcast_ref().unwrap();

        let res_arrow_array = match arrow_array.len() {
            0 => arrow2::array::BooleanArray::new_empty(arrow2::datatypes::DataType::Boolean),
            _ => {
                let result = arrow2::compute::aggregate::max_boolean(arrow_array);
                arrow2::array::BooleanArray::from([result])
            }
        };
        DataArray::new(self.field.clone(), Arc::new(res_arrow_array))
    }
}

impl DaftCompareAggable for &DataArray<NullType> {
    type Output = DaftResult<DataArray<NullType>>;
    fn min(&self) -> Self::Output {
        let arrow_array = self.data();

        let res_arrow_array = match arrow_array.len() {
            0 => arrow2::array::NullArray::new_empty(arrow2::datatypes::DataType::Null),
            _ => arrow2::array::NullArray::new(arrow2::datatypes::DataType::Null, 1),
        };
        DataArray::new(self.field.clone(), Arc::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        // Min and max are the same for NullArray.
        Self::min(self)
    }
}
