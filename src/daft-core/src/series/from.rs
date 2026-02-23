use common_ndarray::NdArray;

use super::Series;
use crate::{prelude::*, series::array_impl::IntoSeries};

impl Series {
    pub fn from_ndarray_flattened(arr: NdArray) -> Self {
        // set a default name for convenience. you can rename if you want a different name
        let name = "ndarray";
        match arr {
            NdArray::I8(arr) => Int8Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::U8(arr) => UInt8Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::I16(arr) => Int16Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::U16(arr) => UInt16Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::I32(arr) => Int32Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::U32(arr) => UInt32Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::I64(arr) => Int64Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::U64(arr) => UInt64Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::F32(arr) => Float32Array::from_vec(name, arr.flatten().to_vec()).into_series(),
            NdArray::F64(arr) => Float64Array::from_vec(name, arr.flatten().to_vec()).into_series(),
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

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

        let arrow_array: Box<dyn daft_arrow::array::Array> = Box::new(arrow_array);
        let field = Arc::new(Field::new(
            "test_map",
            DataType::from(arrow_array.data_type()),
        ));
        let series = Series::from_arrow(field, arrow_array.into())?;

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
