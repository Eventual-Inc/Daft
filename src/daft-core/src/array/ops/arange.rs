use crate::{
    array::DataArray,
    datatypes::{DaftNumericType, Int64Array},
};

use common_error::DaftResult;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn arange<S: AsRef<str>>(name: S, start: i64, end: i64, step: usize) -> DaftResult<Self> {
        if start > end {
            return Err(common_error::DaftError::ValueError(format!(
                "invalid range, start greater than end, {start} vs {end}"
            )));
        }
        let data: Vec<i64> = (start..end).step_by(step).collect();
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::<i64>::from_vec(data));
        let data_array = Int64Array::from((name.as_ref(), arrow_array));
        let casted_array = data_array.cast(&T::get_dtype())?;
        let downcasted = casted_array.downcast::<DataArray<T>>()?;
        Ok(downcasted.clone())
    }
}
