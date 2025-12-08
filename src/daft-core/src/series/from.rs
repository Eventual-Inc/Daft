use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_ndarray::NdArray;
use daft_arrow::datatypes::ArrowDataType;
use daft_schema::{dtype::DaftDataType, field::DaftField};

use super::Series;
use crate::{
    array::ops::from_arrow::FromArrow, datatypes::Field, prelude::*,
    series::array_impl::IntoSeries, with_match_daft_types,
};

impl Series {
    pub fn try_from_field_and_arrow_array(
        field: impl Into<Arc<DaftField>>,
        array: Box<dyn daft_arrow::array::Array>,
    ) -> DaftResult<Self> {
        let field = field.into();
        let dtype = &field.dtype;

        with_match_daft_types!(dtype, |$T| {
            Ok(<$T as DaftDataType>::ArrayType::from_arrow(field, array)?.into_series())
        })
    }

    pub fn from_ndarray_flattened(arr: NdArray) -> Self {
        // set a default name for convenience. you can rename if you want a different name
        let name = "ndarray";
        match arr {
            NdArray::I8(arr) => Int8Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::U8(arr) => UInt8Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::I16(arr) => Int16Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::U16(arr) => UInt16Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::I32(arr) => Int32Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::U32(arr) => UInt32Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::I64(arr) => Int64Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::U64(arr) => UInt64Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::F32(arr) => Float32Array::from((name, arr.flatten().to_vec())).into_series(),
            NdArray::F64(arr) => Float64Array::from((name, arr.flatten().to_vec())).into_series(),
            #[cfg(feature = "python")]
            NdArray::Py(arr) => {
                use pyo3::{Python, types::PyList};

                use crate::python::PySeries;

                Python::attach(|py| {
                    let pylist = PyList::new(py, arr.iter().map(|obj| obj.0.as_ref())).unwrap();
                    PySeries::from_pylist(&pylist, Some(name), None)
                        .unwrap()
                        .series
                })
            }
        }
    }
}

impl TryFrom<(&str, Box<dyn daft_arrow::array::Array>)> for Series {
    type Error = DaftError;

    fn try_from((name, array): (&str, Box<dyn daft_arrow::array::Array>)) -> DaftResult<Self> {
        let source_arrow_type: &ArrowDataType = array.data_type();
        let dtype = DaftDataType::from(source_arrow_type);

        let field = Arc::new(Field::new(name, dtype));
        Self::try_from_field_and_arrow_array(field, array)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use common_error::DaftResult;
    use daft_arrow::{
        array::Array,
        datatypes::{ArrowDataType, ArrowField},
    };
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
        use daft_arrow::array::MapArray;

        use super::*;

        let arrow_array = MapArray::new(
            ARROW_DATA_TYPE.clone(),
            vec![0, 1].try_into().unwrap(),
            Box::new(daft_arrow::array::StructArray::new(
                ArrowDataType::Struct(vec![
                    ArrowField::new("key", ArrowDataType::LargeUtf8, false),
                    ArrowField::new("value", ArrowDataType::Date32, true),
                ]),
                vec![
                    Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(["key1"])),
                    daft_arrow::array::Int32Array::from_slice([1])
                        .convert_logical_type(ArrowDataType::Date32),
                ],
                None,
            )),
            None,
        );

        let series = Series::try_from((
            "test_map",
            Box::new(arrow_array) as Box<dyn daft_arrow::array::Array>,
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
