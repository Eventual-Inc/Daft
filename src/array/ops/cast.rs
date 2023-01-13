use arrow2::compute::cast::{can_cast_types, cast, CastOptions};

use crate::{
    array::data_array::{BaseArray, DataArray},
    datatypes::{DaftDataType, DataType},
    error::{DaftError, DaftResult},
    series::Series,
    with_match_arrow_daft_types,
};

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Series> {
        // if self.data_type().eq(dtype) {
        //     let c = self.clone();
        //     return Ok(DataArray::<T>::from(self.data().to_boxed()).into_series());
        // }

        let _arrow_type = dtype.to_arrow();

        if !dtype.is_arrow() || !self.data_type().is_arrow() {
            return Err(DaftError::TypeError(format!(
                "can not cast {:?} to type: {:?}",
                T::get_dtype(),
                dtype
            )));
        }

        let self_arrow_type = self.data_type().to_arrow()?;
        let target_arrow_type = dtype.to_arrow()?;
        if !can_cast_types(&self_arrow_type, &target_arrow_type) {
            return Err(DaftError::TypeError(format!(
                "can not cast {:?} to type: {:?}",
                T::get_dtype(),
                dtype
            )));
        }

        let result_array = cast(
            self.data(),
            &target_arrow_type,
            CastOptions {
                wrapped: true,
                partial: false,
            },
        )?;
        println!(
            "dtype {:?} vs T: {:?} vs {:?}",
            result_array.data_type(),
            target_arrow_type,
            dtype
        );

        Ok(
            with_match_arrow_daft_types!(dtype, |$T| DataArray::<$T>::from(result_array).into_series()),
        )
    }
}
