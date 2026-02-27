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

    use arrow::{
        array::{ArrayRef, Date32Array, LargeStringArray, MapArray},
        buffer::OffsetBuffer,
        datatypes::{DataType as ArrowDataType, Field as ArrowField},
    };
    use common_error::DaftResult;
    use daft_schema::{dtype::DataType, field::Field};

    use crate::series::Series;

    static ARROW_DATA_TYPE: LazyLock<ArrowDataType> = LazyLock::new(|| {
        ArrowDataType::Map(
            Arc::new(ArrowField::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        Arc::new(ArrowField::new("key", ArrowDataType::LargeUtf8, false)),
                        Arc::new(ArrowField::new("value", ArrowDataType::Date32, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    });

    #[test]
    fn test_map_type_conversion() {
        let arrow_data_type = ARROW_DATA_TYPE.clone();
        let dtype = DataType::try_from(&arrow_data_type).unwrap();
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
        let inner = arrow::array::StructArray::new(
            vec![
                Arc::new(ArrowField::new("key", ArrowDataType::LargeUtf8, false)),
                Arc::new(ArrowField::new("value", ArrowDataType::Date32, true)),
            ]
            .into(),
            vec![
                Arc::new(LargeStringArray::from_iter_values(["key1"])),
                Arc::new(Date32Array::from_iter_values([1])),
            ],
            None,
        );
        let ArrowDataType::Map(field, _) = &*ARROW_DATA_TYPE else {
            unreachable!()
        };
        let arrow_field = Arc::new(ArrowField::new("test_map", ARROW_DATA_TYPE.clone(), false));

        let arrow_array = MapArray::new(
            field.clone(),
            OffsetBuffer::from_lengths(vec![0, 1]),
            inner,
            None,
            false,
        );

        let arrow_array: ArrayRef = Arc::new(arrow_array);
        let field = Arc::new(Field::try_from(arrow_field.as_ref()).unwrap());
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
