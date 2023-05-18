use std::sync::Arc;

use crate::{
    datatypes::{logical::LogicalArray, DataType, Field},
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

        let physical_type = dtype.to_physical();

        if dtype.is_logical() {
            let arrow_physical_type = physical_type.to_arrow()?;
            let casted_array = arrow2::compute::cast::cast(
                array.as_ref(),
                &arrow_physical_type,
                arrow2::compute::cast::CastOptions {
                    wrapped: true,
                    partial: false,
                },
            )?;

            return Ok(with_match_daft_logical_types!(dtype, |$T| {
                let physical = DataArray::try_from((Field::new(name, physical_type), casted_array))?;
                LogicalArray::<$T>::new(field, physical).into_series()
            }));
        }

        // is not logical but contains one
        if physical_type != dtype {
            let arrow_physical_type = physical_type.to_arrow()?;
            let casted_array = arrow2::compute::cast::cast(
                array.as_ref(),
                &arrow_physical_type,
                arrow2::compute::cast::CastOptions {
                    wrapped: true,
                    partial: false,
                },
            )?;
            return Ok(
                with_match_physical_daft_types!(physical_type, |$T| DataArray::<$T>::new(field, casted_array)?.into_series()),
            );
        }

        Ok(
            with_match_physical_daft_types!(dtype, |$T| DataArray::<$T>::new(field, array.into())?.into_series()),
        )
    }
}
