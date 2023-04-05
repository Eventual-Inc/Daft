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

        let result = arrow2::compute::aggregate::min_primitive(primitive_arr);
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([result]));

        DataArray::new(self.field.clone(), arrow_array)
    }

    fn max(&self) -> Self::Output {
        let primitive_arr = self.downcast();

        let result = arrow2::compute::aggregate::max_primitive(primitive_arr);
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([result]));

        DataArray::new(self.field.clone(), arrow_array)
    }
}

impl DaftCompareAggable for &DataArray<Utf8Type> {
    type Output = DaftResult<DataArray<Utf8Type>>;
    fn min(&self) -> Self::Output {
        let arrow_array: &arrow2::array::Utf8Array<i64> =
            self.data().as_any().downcast_ref().unwrap();

        let result = arrow2::compute::aggregate::min_string(arrow_array);
        let res_arrow_array = arrow2::array::Utf8Array::<i64>::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        let arrow_array: &arrow2::array::Utf8Array<i64> =
            self.data().as_any().downcast_ref().unwrap();

        let result = arrow2::compute::aggregate::max_string(arrow_array);
        let res_arrow_array = arrow2::array::Utf8Array::<i64>::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }
}

impl DaftCompareAggable for &DataArray<BooleanType> {
    type Output = DaftResult<DataArray<BooleanType>>;
    fn min(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BooleanArray =
            self.data().as_any().downcast_ref().unwrap();

        let result = arrow2::compute::aggregate::min_boolean(arrow_array);
        let res_arrow_array = arrow2::array::BooleanArray::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }
    fn max(&self) -> Self::Output {
        let arrow_array: &arrow2::array::BooleanArray =
            self.data().as_any().downcast_ref().unwrap();

        let result = arrow2::compute::aggregate::max_boolean(arrow_array);
        let res_arrow_array = arrow2::array::BooleanArray::from([result]);

        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }
}

impl DaftCompareAggable for &DataArray<NullType> {
    type Output = DaftResult<DataArray<NullType>>;

    fn min(&self) -> Self::Output {
        let res_arrow_array = arrow2::array::NullArray::new(arrow2::datatypes::DataType::Null, 1);
        DataArray::new(self.field.clone(), Box::new(res_arrow_array))
    }

    fn max(&self) -> Self::Output {
        // Min and max are the same for NullArray.
        Self::min(self)
    }
}
