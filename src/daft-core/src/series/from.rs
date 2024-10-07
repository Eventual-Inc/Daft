use std::sync::Arc;

use arrow2::datatypes::ArrowDataType;
use common_error::{DaftError, DaftResult};
use daft_schema::{dtype::DaftDataType, field::DaftField};

use super::Series;
use crate::{
    array::ops::from_arrow::FromArrow,
    datatypes::{DataType, Field},
    series::array_impl::IntoSeries,
    with_match_daft_types,
};

impl Series {
    pub fn try_from_field_and_arrow_array(
        field: impl Into<Arc<DaftField>>,
        array: Box<dyn arrow2::array::Array>,
    ) -> DaftResult<Self> {
        let field = field.into();
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

    fn try_from((name, array): (&str, Box<dyn arrow2::array::Array>)) -> DaftResult<Self> {
        let source_arrow_type: &ArrowDataType = array.data_type();
        let dtype = DaftDataType::from(source_arrow_type);

        let field = Arc::new(Field::new(name, dtype));
        Self::try_from_field_and_arrow_array(field, array)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use arrow2::{
        array::Array,
        datatypes::{ArrowDataType, ArrowField},
    };
    use common_error::DaftResult;
    use daft_schema::dtype::DataType;

    static ARROW_DATA_TYPE: LazyLock<ArrowDataType> = LazyLock::new(|| {
        ArrowDataType::Map(
            Box::new(ArrowField::new(
                "entries",
                ArrowDataType::Struct(vec![
                    ArrowField::new("key", ArrowDataType::LargeUtf8, false),
                    ArrowField::new("value", ArrowDataType::Date32, true),
                ]),
                false,
            )),
            false,
        )
    });

    #[test]
    fn test_map_type_conversion() {
        let arrow_data_type = ARROW_DATA_TYPE.clone();
        let dtype = DataType::from(&arrow_data_type);
        assert_eq!(
            dtype,
            DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(DataType::Date),
            },
        );
    }

    #[test]
    fn test_map_array_conversion() -> DaftResult<()> {
        use arrow2::array::MapArray;

        use super::*;

        let arrow_array = MapArray::new(
            ARROW_DATA_TYPE.clone(),
            vec![0, 1].try_into().unwrap(),
            Box::new(arrow2::array::StructArray::new(
                ArrowDataType::Struct(vec![
                    ArrowField::new("key", ArrowDataType::LargeUtf8, false),
                    ArrowField::new("value", ArrowDataType::Date32, true),
                ]),
                vec![
                    Box::new(arrow2::array::Utf8Array::<i64>::from_slice(["key1"])),
                    arrow2::array::Int32Array::from_slice([1])
                        .convert_logical_type(ArrowDataType::Date32),
                ],
                None,
            )),
            None,
        );

        let series = Series::try_from((
            "test_map",
            Box::new(arrow_array) as Box<dyn arrow2::array::Array>,
        ))?;

        assert_eq!(
            series.data_type(),
            &DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(DataType::Date),
            }
        );

        Ok(())
    }
}
