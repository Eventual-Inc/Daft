use std::sync::Arc;

use crate::{
    datatypes::{DataType, Field},
    with_match_daft_types,
};
use common_error::{DaftError, DaftResult};

use super::Series;

use crate::array::ops::from_arrow::FromArrow;
use crate::series::array_impl::IntoSeries;

impl Series {
    pub fn try_from_field_and_arrow_array(
        field: Arc<Field>,
        array: Box<dyn arrow2::array::Array>,
    ) -> DaftResult<Self> {
        // TODO(Nested): Refactor this out with nested logical types in StructArray and ListArray
        // Corner-case nested logical types that have not yet been migrated to new Array formats
        // to hold only casted physical arrow arrays.
        let dtype = &field.dtype;
        if matches!(dtype, DataType::List(..) | DataType::Extension(..))
            && let physical_type = dtype.to_physical()
            && &physical_type != dtype
        {
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
                with_match_daft_types!(physical_type, |$T| <$T as DaftDataType>::ArrayType::from_arrow(field, casted_array)?.into_series()),
            );
        }

        with_match_daft_types!(dtype, |$T| {
            Ok(<$T as DaftDataType>::ArrayType::from_arrow(field, array)?.into_series())
        })
    }
}

impl TryFrom<(&str, Box<dyn arrow2::array::Array>)> for Series {
    type Error = DaftError;

    fn try_from(item: (&str, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let (name, array) = item;
        let source_arrow_type = array.data_type();
        let dtype: DataType = source_arrow_type.into();
        let field = Arc::new(Field::new(name, dtype.clone()));
        Self::try_from_field_and_arrow_array(field, array)
    }
}
