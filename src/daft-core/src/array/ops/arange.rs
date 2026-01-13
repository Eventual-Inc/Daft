use common_error::DaftResult;

use crate::{
    array::DataArray,
    datatypes::{DaftNumericType, Int64Array},
};

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
        let arr = Int64Array::from_iter_values((start..end).step_by(step)).rename(name.as_ref());

        let casted_array = arr.cast(&T::get_dtype())?;
        let downcasted = casted_array.downcast::<Self>()?;
        Ok(downcasted.clone())
    }
}
