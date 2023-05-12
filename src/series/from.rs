use std::sync::Arc;

use crate::{
    datatypes::{
        logical::{DateArray, LogicalArray},
        DataType, DateType, Field, Int32Array,
    },
    error::{DaftError, DaftResult},
    with_match_daft_logical_types, with_match_physical_daft_types,
};

use super::Series;

use crate::series::array_impl::IntoSeries;

impl TryFrom<(&str, Box<dyn arrow2::array::Array>)> for Series {
    type Error = DaftError;

    fn try_from(item: (&str, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let (name, array) = item;
        let self_arrow_type = array.data_type();
        let dtype: DataType = self_arrow_type.into();
        let field = Arc::new(Field::new(name, dtype.clone()));

        if dtype.is_logical() {
            return Ok(with_match_daft_logical_types!(dtype, |$T| {
                let arrow_physical_type = dtype.to_physical().to_arrow()?;
                let casted_array = arrow2::compute::cast::cast(array.as_ref(), &arrow_physical_type,
                arrow2::compute::cast::CastOptions {
                    wrapped: true,
                    partial: false,
                })?;
                let physical = DataArray::try_from((name, casted_array))?;
                LogicalArray::<$T>::new(field, physical).into_series()
            }));
        }
        Ok(
            with_match_physical_daft_types!(dtype, |$T| DataArray::<$T>::new(field, array.into())?.into_series()),
        )
    }
}
