use std::sync::Arc;

use crate::{
    datatypes::{logical::LogicalArray, DataType, Field},
    with_match_daft_logical_primitive_types, with_match_daft_logical_types,
    with_match_physical_daft_types,
};
use common_error::{DaftError, DaftResult};

use super::Series;

use crate::array::ops::from_arrow::FromArrow;
use crate::series::array_impl::IntoSeries;

impl TryFrom<(&str, Box<dyn arrow2::array::Array>)> for Series {
    type Error = DaftError;

    fn try_from(item: (&str, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let (name, array) = item;
        let source_arrow_type = array.data_type();
        let dtype: DataType = source_arrow_type.into();
        let field = Arc::new(Field::new(name, dtype.clone()));

        let physical_type = dtype.to_physical();

        if dtype.is_logical() {
            let arrow_physical_type = physical_type.to_arrow()?;

            use DataType::*;
            let physical_arrow_array = match dtype {
                // Primitive wrapper types: change the arrow2 array's type field to primitive
                Decimal128(..) | Date | Timestamp(..) | Duration(..) => {
                    with_match_daft_logical_primitive_types!(dtype, |$P| {
                        use arrow2::array::Array;
                        array
                            .as_any()
                            .downcast_ref::<arrow2::array::PrimitiveArray<$P>>()
                            .unwrap()
                            .clone()
                            .to(arrow_physical_type)
                            .to_boxed()
                    })
                }
                // Otherwise, use an Arrow cast to drop Extension types.
                _ => arrow2::compute::cast::cast(
                    array.as_ref(),
                    &arrow_physical_type,
                    arrow2::compute::cast::CastOptions {
                        wrapped: true,
                        partial: false,
                    },
                )?,
            };

            let res = with_match_daft_logical_types!(dtype, |$T| {
                LogicalArray::<$T>::from_arrow(field.as_ref(), physical_arrow_array)?.into_series()
            });
            return Ok(res);
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
                with_match_physical_daft_types!(physical_type, |$T| DataArray::<$T>::from_arrow(field.as_ref(), casted_array)?.into_series()),
            );
        }

        Ok(
            with_match_physical_daft_types!(dtype, |$T| DataArray::<$T>::from_arrow(field.as_ref(), array.into())?.into_series()),
        )
    }
}
