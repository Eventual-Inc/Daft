use common_error::{DaftError, DaftResult};
use common_file::{DaftFile, DaftFileType};
use common_image::CowImage;

use crate::{
    array::ops::image::image_array_from_img_buffers, datatypes::logical::FileArray, prelude::*,
};

impl TryFrom<Vec<Literal>> for Series {
    type Error = DaftError;

    fn try_from(values: Vec<Literal>) -> DaftResult<Self> {
        let dtype = values
            .iter()
            .try_fold(DataType::Null, |acc, v| match (acc, v.get_type()) {
                (dtype, DataType::Null) | (DataType::Null, dtype) => Ok(dtype),
                (acc, dtype) if acc == dtype => Ok(acc),
                (acc, dtype) => Err(DaftError::ValueError(format!(
                    "All literals must have the same data type or null. Found: {} vs {}",
                    acc, dtype
                ))),
            })?;

        let field = Field::new("literal", dtype.clone());

        macro_rules! unwrap_inner {
            ($expr:expr, $variant:ident) => {
                match $expr {
                    Literal::$variant(val) => Some(val),
                    Literal::Null => None,
                    _ => unreachable!("datatype is already checked"),
                }
            };

            ($expr:expr, $literal:pat => $value:expr) => {
                match $expr {
                    $literal => Some($value),
                    Literal::Null => None,
                    _ => unreachable!("datatype is already checked"),
                }
            };
        }

        macro_rules! from_iter_with_str {
            ($arr_type:ty, $variant:ident) => {{
                <$arr_type>::from_iter(
                    "literal",
                    values.into_iter().map(|lit| unwrap_inner!(lit, $variant)),
                )
                .into_series()
            }};
        }

        macro_rules! from_iter_with_field {
            ($arr_type:ty, $variant:ident) => {{
                <$arr_type>::from_iter(
                    field,
                    values.into_iter().map(|lit| unwrap_inner!(lit, $variant)),
                )
                .into_series()
            }};
        }

        Ok(match dtype {
            DataType::Null => NullArray::full_null("literal", &dtype, values.len()).into_series(),
            DataType::Boolean => from_iter_with_str!(BooleanArray, Boolean),
            DataType::Utf8 => from_iter_with_str!(Utf8Array, Utf8),
            DataType::Binary => from_iter_with_str!(BinaryArray, Binary),
            DataType::Int8 => from_iter_with_field!(Int8Array, Int8),
            DataType::UInt8 => from_iter_with_field!(UInt8Array, UInt8),
            DataType::Int16 => from_iter_with_field!(Int16Array, Int16),
            DataType::UInt16 => from_iter_with_field!(UInt16Array, UInt16),
            DataType::Int32 => from_iter_with_field!(Int32Array, Int32),
            DataType::UInt32 => from_iter_with_field!(UInt32Array, UInt32),
            DataType::Int64 => from_iter_with_field!(Int64Array, Int64),
            DataType::UInt64 => from_iter_with_field!(UInt64Array, UInt64),
            DataType::Interval => from_iter_with_str!(IntervalArray, Interval),
            DataType::Float32 => from_iter_with_field!(Float32Array, Float32),
            DataType::Float64 => from_iter_with_field!(Float64Array, Float64),
            DataType::Decimal128 { .. } => Decimal128Array::from_iter(
                field,
                values
                    .into_iter()
                    .map(|lit| unwrap_inner!(lit, Literal::Decimal(v, ..) => v)),
            )
            .into_series(),
            DataType::Timestamp(_, _) => {
                let data = values
                    .into_iter()
                    .map(|lit| unwrap_inner!(lit, Literal::Timestamp(ts, ..) => ts));
                let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);
                TimestampArray::new(field, physical).into_series()
            }
            DataType::Date => {
                let data = values.into_iter().map(|lit| unwrap_inner!(lit, Date));
                let physical = Int32Array::from_iter(Field::new("literal", DataType::Int32), data);
                DateArray::new(field, physical).into_series()
            }
            DataType::Time(_) => {
                let data = values
                    .into_iter()
                    .map(|lit| unwrap_inner!(lit, Literal::Time(t, ..) => t));
                let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);

                TimeArray::new(field, physical).into_series()
            }
            DataType::Duration(_) => {
                let data = values
                    .into_iter()
                    .map(|lit| unwrap_inner!(lit, Literal::Duration(v, ..) => v));
                let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);

                DurationArray::new(field, physical).into_series()
            }
            DataType::List(_) => {
                let data = values
                    .iter()
                    .map(|v| unwrap_inner!(v, List))
                    .collect::<Vec<_>>();
                ListArray::try_from(("literal", data.as_slice()))?.into_series()
            }
            DataType::Struct(fields) => {
                let children = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        let child_values = values
                            .iter()
                            .map(|v| match unwrap_inner!(v, Struct) {
                                Some(v) => v[i].clone(),
                                None => Literal::Null,
                            })
                            .collect::<Vec<_>>();

                        Ok(Self::try_from(child_values)?.rename(&f.name))
                    })
                    .collect::<DaftResult<_>>()?;

                let validity = arrow2::bitmap::Bitmap::from_trusted_len_iter(
                    values.iter().map(|v| *v != Literal::Null),
                );

                StructArray::new(field, children, Some(validity)).into_series()
            }

            #[cfg(feature = "python")]
            DataType::Python => {
                use std::sync::Arc;

                let pynone = Arc::new(pyo3::Python::with_gil(|py| py.None()));

                let data = values.into_iter().map(|lit| {
                    unwrap_inner!(lit, Python).map_or_else(|| pynone.clone(), |pyobj| pyobj.0)
                });

                PythonArray::from(("literal", data.collect::<Vec<_>>())).into_series()
            }
            DataType::File => {
                let (discriminant, values) = values
                    .into_iter()
                    .map(|v| match v {
                        Literal::File(DaftFile::Reference(path)) => {
                            (DaftFileType::Reference as u8, path.into_bytes())
                        }
                        Literal::File(DaftFile::Data(bytes)) => (DaftFileType::Data as u8, bytes),
                        _ => panic!("should not happen"),
                    })
                    .unzip::<_, _, Vec<u8>, Vec<_>>();
                let discriminant_field = Field::new("discriminant", DataType::UInt8);
                let values_field = Field::new("data", DataType::Binary);
                let discriminant = UInt8Array::from_values_iter(
                    discriminant_field.clone(),
                    discriminant.into_iter(),
                )
                .into_series();
                let values =
                    BinaryArray::from_values(&values_field.name, values.into_iter()).into_series();
                let fld = Field::new(
                    "literal",
                    DataType::Struct(vec![discriminant_field, values_field]),
                );
                let sa = StructArray::new(fld, vec![discriminant, values], None);
                FileArray::new(Field::new("literal", DataType::File), sa).into_series()
            }
            DataType::Tensor(_) => {
                let (data, shapes) = values
                    .iter()
                    .map(|v| {
                        unwrap_inner!(v, Literal::Tensor { data, shape } => (data, shape.as_slice())).unzip()
                    })
                    .collect::<(Vec<_>, Vec<_>)>();

                let data_array = ListArray::try_from(("data", data.as_slice()))?.into_series();
                let shape_array = ListArray::from(("shape", shapes.as_slice())).into_series();

                let validity = data_array.validity().cloned();
                let physical =
                    StructArray::new(field.to_physical(), vec![data_array, shape_array], validity);

                TensorArray::new(field, physical).into_series()
            }
            DataType::SparseTensor(..) => {
                let (values, indices, shapes) = values
                    .iter()
                    .map(|v| {
                        match unwrap_inner!(v, Literal::SparseTensor { values, indices, shape, .. } => (values, indices, shape)) {
                            Some((v, i, s)) => (Some(v), Some(i), Some(s.as_slice())),
                            None => (None, None, None)
                        }
                    })
                    .collect::<(Vec<_>, Vec<_>, Vec<_>)>();

                let values_array =
                    ListArray::try_from(("values", values.as_slice()))?.into_series();
                let indices_array =
                    ListArray::try_from(("indices", indices.as_slice()))?.into_series();
                let shape_array = ListArray::from(("shape", shapes.as_slice())).into_series();

                let validity = values_array.validity().cloned();
                let physical = StructArray::new(
                    field.to_physical(),
                    vec![values_array, indices_array, shape_array],
                    validity,
                );

                SparseTensorArray::new(field, physical).into_series()
            }
            DataType::Embedding(inner_dtype, size) => {
                let validity = arrow2::bitmap::Bitmap::from_trusted_len_iter(
                    values.iter().map(|v| *v != Literal::Null),
                );

                let data = values
                    .into_iter()
                    .map(|v| {
                        unwrap_inner!(v, Embedding)
                            .unwrap_or_else(|| Self::full_null("literal", &inner_dtype, size))
                    })
                    .collect::<Vec<_>>();
                let flat_child = Self::concat(&data.iter().collect::<Vec<_>>())?;

                let physical =
                    FixedSizeListArray::new(field.to_physical(), flat_child, Some(validity));

                EmbeddingArray::new(field, physical).into_series()
            }
            DataType::Map { .. } => {
                let data = values
                    .into_iter()
                    .map(|v| {
                        unwrap_inner!(v, Literal::Map { keys, values } => (keys, values)).map(
                            |(k, v)| {
                                StructArray::new(
                                    field
                                        .to_physical()
                                        .to_exploded_field()
                                        .expect("Expected physical type of map to be list"),
                                    vec![k.rename("key"), v.rename("value")],
                                    None,
                                )
                                .into_series()
                            },
                        )
                    })
                    .collect::<Vec<_>>();

                let physical = ListArray::try_from(("literal", data.as_slice()))?;

                MapArray::new(field, physical).into_series()
            }
            DataType::Image(image_mode) => {
                let data = values
                    .iter()
                    .map(|v| unwrap_inner!(v, Image).map(|img| CowImage::from(&img.0)))
                    .collect::<Vec<_>>();

                image_array_from_img_buffers("literal", &data, image_mode)?.into_series()
            }

            DataType::FixedSizeBinary(..)
            | DataType::FixedSizeList(..)
            | DataType::Extension(..)
            | DataType::FixedShapeImage(..)
            | DataType::FixedShapeTensor(..)
            | DataType::FixedShapeSparseTensor(..)
            | DataType::Unknown => unreachable!("Literal should never have data type: {dtype}"),
        })
    }
}

impl From<Literal> for Series {
    fn from(value: Literal) -> Self {
        Self::try_from(vec![value])
            .expect("Series::try_from should not fail on single literal value")
    }
}

#[cfg(test)]
mod test {
    use common_image::Image;
    use image::{GrayImage, RgbaImage};
    use indexmap::indexmap;
    use rstest::rstest;

    use crate::{datatypes::IntervalValue, prelude::*, series};

    #[rstest]
    #[case::null(vec![Literal::Null, Literal::Null])]
    #[case::bool(vec![Literal::Boolean(true), Literal::Boolean(false)])]
    #[case::utf8(vec![Literal::Utf8("foo".to_string()), Literal::Utf8("bar".to_string())])]
    #[case::binary(vec![Literal::Binary(vec![]), Literal::Binary(vec![1]), Literal::Binary(vec![2, 3])])]
    #[case::int8(vec![Literal::Int8(0), Literal::Int8(1), Literal::Int8(-2)])]
    #[case::uint8(vec![Literal::UInt8(0), Literal::UInt8(1)])]
    #[case::int16(vec![Literal::Int16(0), Literal::Int16(1), Literal::Int16(-2)])]
    #[case::uint16(vec![Literal::UInt16(0), Literal::UInt16(1)])]
    #[case::int32(vec![Literal::Int32(0), Literal::Int32(1), Literal::Int32(-2)])]
    #[case::uint32(vec![Literal::UInt32(0), Literal::UInt32(1)])]
    #[case::int64(vec![Literal::Int64(0), Literal::Int64(1), Literal::Int64(-2)])]
    #[case::uint64(vec![Literal::UInt64(0), Literal::UInt64(1)])]
    #[case::ts_us(vec![
        Literal::Timestamp(0, TimeUnit::Microseconds, None),
        Literal::Timestamp(1, TimeUnit::Microseconds, None),
        Literal::Timestamp(-2, TimeUnit::Microseconds, None)
    ])]
    #[case::ts_s_tz(vec![
        Literal::Timestamp(0, TimeUnit::Seconds, Some("America/Los_Angeles".to_string())),
        Literal::Timestamp(1, TimeUnit::Seconds, Some("America/Los_Angeles".to_string())),
        Literal::Timestamp(-2, TimeUnit::Seconds, Some("America/Los_Angeles".to_string()))
    ])]
    #[case::date(vec![Literal::Date(0), Literal::Date(1), Literal::Date(-2)])]
    #[case::time_us(vec![
        Literal::Time(0, TimeUnit::Microseconds),
        Literal::Time(1, TimeUnit::Microseconds),
        Literal::Time(-2, TimeUnit::Microseconds)
    ])]
    #[case::time_s(vec![
        Literal::Time(0, TimeUnit::Seconds),
        Literal::Time(1, TimeUnit::Seconds),
        Literal::Time(-2, TimeUnit::Seconds)
    ])]
    #[case::duration_us(vec![
        Literal::Duration(0, TimeUnit::Microseconds),
        Literal::Duration(1, TimeUnit::Microseconds),
        Literal::Duration(-2, TimeUnit::Microseconds)
    ])]
    #[case::duration_s(vec![
        Literal::Duration(0, TimeUnit::Seconds),
        Literal::Duration(1, TimeUnit::Seconds),
        Literal::Duration(-2, TimeUnit::Seconds)
    ])]
    #[case::interval(vec![Literal::Interval(IntervalValue::new(0, 0, 0)), Literal::Interval(IntervalValue::new(1, -2, 3))])]
    #[case::float32(vec![Literal::Float32(0.0), Literal::Float32(0.5), Literal::Float32(-1.5)])]
    #[case::float64(vec![Literal::Float64(0.0), Literal::Float64(0.5), Literal::Float64(-1.5)])]
    #[case::decimal_1(vec![Literal::Decimal(0, 0, 1), Literal::Decimal(1, 0, 1), Literal::Decimal(-2, 0, 1)])]
    #[case::decimal_2(vec![Literal::Decimal(0, 5, 10), Literal::Decimal(1, 5, 10), Literal::Decimal(-2, 5, 10)])]
    #[case::list(vec![Literal::List(Series::empty("literal", &DataType::Int32)), Literal::List(series![1, 2, 3])])]
    #[case::struct_(vec![
        Literal::Struct(indexmap!{Field::new("a", DataType::Boolean) => Literal::Boolean(true)}),
        Literal::Struct(indexmap!{Field::new("a", DataType::Boolean) => Literal::Boolean(false)}),
    ])]
    #[case::tensor(vec![
        Literal::Tensor { data: series![0, 0], shape: vec![1, 2] },
        Literal::Tensor { data: series![1, 2, 3, 4, 5, 6], shape: vec![1, 2, 3] },
    ])]
    #[case::sparse_tensor_no_offset(vec![
        Literal::SparseTensor {
            values: series![0],
            indices: series![0].cast(&DataType::UInt64).unwrap(),
            shape: vec![1, 2],
            indices_offset: false
        },
        Literal::SparseTensor {
            values: series![1, 2, 3, 4],
            indices: series![1, 2, 3, 4].cast(&DataType::UInt64).unwrap(),
            shape: vec![1, 2, 3],
            indices_offset: false
        },
    ])]
    #[case::sparse_tensor_with_offset(vec![
        Literal::SparseTensor {
            values: series![0],
            indices: series![0].cast(&DataType::UInt64).unwrap(),
            shape: vec![1, 2],
            indices_offset: true
        },
        Literal::SparseTensor {
            values: series![1, 2, 3, 4],
            indices: series![1, 1, 1, 1].cast(&DataType::UInt64).unwrap(),
            shape: vec![1, 2, 3],
            indices_offset: true
        },
    ])]
    #[case::embedding(vec![
        Literal::Embedding(series![0, 0, 0, 0]),
        Literal::Embedding(series![1, 2, 3, 4]),
    ])]
    #[case::map(vec![
        Literal::Map { keys: series!["a", "b", "c"], values: series![1, 2, 3] },
        Literal::Map { keys: series!["d", "e"], values: series![4, 5] },
        Literal::Map { keys: Series::empty("literal", &DataType::Utf8), values: Series::empty("literal", &DataType::Int32) },
    ])]
    #[case::image_gray(vec![
        Literal::Image(Image(GrayImage::new(1, 1).into())),
        Literal::Image(Image(GrayImage::new(100, 100).into())),
        Literal::Image(Image(GrayImage::from_raw(10, 10, vec![10; 100]).unwrap().into())),
    ])]
    #[case::image_rgba(vec![
        Literal::Image(Image(RgbaImage::new(1, 1).into())),
        Literal::Image(Image(RgbaImage::new(100, 100).into())),
        Literal::Image(Image(RgbaImage::from_raw(10, 10, vec![10; 400]).unwrap().into())),
    ])]
    fn test_literal_series_roundtrip_basics(#[case] literals: Vec<Literal>) {
        let expected = [vec![Literal::Null], literals, vec![Literal::Null]].concat();
        let series = Series::try_from(expected.clone()).unwrap();
        let actual = series.to_literals().collect::<Vec<_>>();

        assert_eq!(expected, actual)
    }

    #[test]
    fn test_literals_to_series_mismatched() {
        let values = vec![Literal::UInt64(1), Literal::Utf8("test".to_string())];
        let actual = Series::try_from(values);
        assert!(actual.is_err());
    }
}
