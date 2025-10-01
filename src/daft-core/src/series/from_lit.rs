use std::sync::Arc;

use arrow2::{
    array::{MutableArray, MutablePrimitiveArray},
    trusted_len::TrustedLen,
};
use common_error::{DaftError, DaftResult};
use common_image::CowImage;

use crate::{array::ops::image::image_array_from_img_buffers, datatypes::FileArray, prelude::*};

/// Combine literal types that are likely the same but do not equal due to lossy literal casting.
///
/// For example, null literals are always null type, and image literals always have a mode.
pub(crate) fn combine_lit_types(left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (dtype, DataType::Null) | (DataType::Null, dtype) => Some(dtype.clone()),
        (left, right) if left == right => Some(left.clone()),
        (DataType::List(left_child), DataType::List(right_child)) => {
            combine_lit_types(left_child, right_child).map(|child| DataType::List(Box::new(child)))
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            if left_fields.len() != right_fields.len() {
                return None;
            }

            let fields = left_fields
                .iter()
                .zip(right_fields)
                .map(|(l, r)| {
                    if l.name != r.name {
                        return None;
                    }

                    let field_dtype = combine_lit_types(&l.dtype, &r.dtype)?;
                    Some(Field::new(l.name.clone(), field_dtype))
                })
                .collect::<Option<_>>()?;

            Some(DataType::Struct(fields))
        }
        (
            DataType::Map {
                key: left_key,
                value: left_value,
            },
            DataType::Map {
                key: right_key,
                value: right_value,
            },
        ) => {
            let key = Box::new(combine_lit_types(left_key, right_key)?);
            let value = Box::new(combine_lit_types(left_value, right_value)?);

            Some(DataType::Map { key, value })
        }
        (DataType::Image(_), DataType::Image(_)) => Some(DataType::Image(None)),
        _ => None,
    }
}

/// How to handle errors when combining literals.
pub enum OnError {
    /// Errors result in Null values
    Null,
    /// Errors will be raised
    Raise,
}

impl Series {
    pub fn from_literals_iter<I: ExactSizeIterator<Item = DaftResult<Literal>> + TrustedLen>(
        values: I,
        dtype: DataType,
        on_error: OnError,
    ) -> DaftResult<Self> {
        let field = Field::new("literal", dtype.clone());

        // DaftError isn't `Clone` so just using a string to store the error instead.
        let mut err: Option<String> = None;

        macro_rules! unwrap_inner {
            ($expr:expr, $variant:ident) => {
                match $expr {
                    Ok(Literal::$variant(val)) => Some(val),
                    Ok(Literal::Null) => None,
                    Err(e) => {
                        match on_error {
                            OnError::Null => None,
                            OnError::Raise => {
                                err = Some(e.to_string());
                                None
                            },

                        }

                    }
                    // SAFETY: This is safe because we have already checked that all literals have the same data type or null.
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            };

            ($expr:expr, $literal:pat => $value:expr) => {
                match $expr {
                    Ok($literal) => Some($value),
                    Ok(Literal::Null) => None,
                    Err(e) => {
                        match on_error {
                            OnError::Null => None,
                            OnError::Raise => {
                                err = Some(e.to_string());
                                None
                            },

                        }
                    }
                    // SAFETY: This is safe because we have already checked that all literals have the same data type or null.
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            };
        }

        macro_rules! from_iter_with_str {
            ($arr_type:ty, $variant:ident) => {{
                <$arr_type>::from_iter("literal", values.map(|lit| unwrap_inner!(lit, $variant)))
                    .into_series()
            }};
        }

        macro_rules! from_mutable_primitive_array {
            ($arr_type:ty, $variant:ident) => {{
                let mut arr = MutablePrimitiveArray::<$arr_type>::with_capacity(values.len());
                for value in values {
                    let value = unwrap_inner!(value, $variant);
                    arr.push(value);
                }

                Series::from_arrow(Arc::new(field), arr.as_box())?
            }};
        }

        let s = match dtype {
            DataType::Null => NullArray::full_null("literal", &dtype, values.len()).into_series(),
            DataType::Boolean => from_iter_with_str!(BooleanArray, Boolean),
            DataType::Utf8 => from_iter_with_str!(Utf8Array, Utf8),
            DataType::Binary => from_iter_with_str!(BinaryArray, Binary),
            DataType::Int8 => from_mutable_primitive_array!(i8, Int8),
            DataType::UInt8 => from_mutable_primitive_array!(u8, UInt8),
            DataType::Int16 => from_mutable_primitive_array!(i16, Int16),
            DataType::UInt16 => from_mutable_primitive_array!(u16, UInt16),
            DataType::Int32 => from_mutable_primitive_array!(i32, Int32),
            DataType::UInt32 => from_mutable_primitive_array!(u32, UInt32),
            DataType::Int64 => from_mutable_primitive_array!(i64, Int64),
            DataType::UInt64 => from_mutable_primitive_array!(u64, UInt64),
            DataType::Interval => from_iter_with_str!(IntervalArray, Interval),
            DataType::Float32 => from_mutable_primitive_array!(f32, Float32),
            DataType::Float64 => from_mutable_primitive_array!(f64, Float64),

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
            DataType::List(child_dtype) => {
                let data = values
                    .map(|v| {
                        unwrap_inner!(v, List)
                            .map(|s| s.cast(&child_dtype))
                            .transpose()
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                ListArray::try_from(("literal", data.as_slice()))?.into_series()
            }
            DataType::Struct(fields) => {
                let values = values.collect::<Vec<_>>();

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

                        Ok(Self::from_literals(child_values, Some(f.dtype.clone()))?
                            .rename(&f.name))
                    })
                    .collect::<DaftResult<_>>()?;

                let validity = arrow2::bitmap::Bitmap::from_trusted_len_iter(
                    values
                        .iter()
                        .map(|v| v.as_ref().is_ok_and(|v| v != &Literal::Null)),
                );

                StructArray::new(field, children, Some(validity)).into_series()
            }

            #[cfg(feature = "python")]
            DataType::Python => {
                let iter = values
                    .into_iter()
                    .map(|lit| unwrap_inner!(lit, Python).map(|pyobj| pyobj.0));

                PythonArray::from_iter("literal", iter).into_series()
            }
            #[cfg(feature = "python")]
            DataType::File => {
                let values = values.collect::<Vec<_>>();
                use std::sync::Arc;

                use common_file::FileReference;
                use pyo3::IntoPyObjectExt;

                let discriminant = UInt8Array::from_iter(
                    Field::new("discriminant", DataType::UInt8),
                    values
                        .iter()
                        .map(|lit| unwrap_inner!(lit, File).map(|f| f.get_type() as u8)),
                )
                .into_series();
                let validity = discriminant.validity().cloned();

                let (files_and_configs, data): (Vec<_>, Vec<Option<Vec<u8>>>) =
                    pyo3::Python::with_gil(|py| {
                        values
                            .into_iter()
                            .map(|lit| {
                                let Some(f) = unwrap_inner!(lit, File) else {
                                    return ((None, None), None);
                                };

                                match f {
                                    FileReference::Reference(file, ioconfig) => {
                                        let io_conf = ioconfig.as_ref().map(|conf| {
                                            common_io_config::python::IOConfig::from(
                                                conf.as_ref().clone(),
                                            )
                                        });
                                        let io_config =
                                            io_conf.map(|io_conf| {
                                                Arc::new(io_conf.into_py_any(py).expect(
                                                    "Failed to convert ioconfig to PyObject",
                                                ))
                                            });
                                        ((Some(file), io_config), None)
                                    }
                                    FileReference::Data(items) => {
                                        ((None, None), Some(items.as_ref().clone()))
                                    }
                                }
                            })
                            .unzip()
                    });
                let (files, io_confs): (Vec<Option<String>>, Vec<_>) =
                    files_and_configs.into_iter().unzip();
                let sa_field = Field::new("literal", DataType::File.to_physical());
                let io_configs = PythonArray::from_iter("io_config", io_confs.into_iter());
                let urls = Utf8Array::from_iter("url", files.into_iter());
                let data = BinaryArray::from_iter("data", data.into_iter());
                let sa = StructArray::new(
                    sa_field,
                    vec![
                        discriminant,
                        data.into_series(),
                        urls.into_series(),
                        io_configs.into_series(),
                    ],
                    validity,
                );
                FileArray::new(Field::new("literal", DataType::File), sa).into_series()
            }
            #[cfg(not(feature = "python"))]
            DataType::File => unreachable!("File type is only supported with the python feature"),

            DataType::Tensor(_) => {
                let values = values.collect::<Vec<_>>();

                let (data, shapes) = values.iter()
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
                let values = values.collect::<Vec<_>>();

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
                let (validity, data): (Vec<_>, Vec<_>) = values
                    .into_iter()
                    .map(|v| {
                        (
                            v.as_ref().is_ok_and(|v| v != &Literal::Null),
                            unwrap_inner!(v, Embedding)
                                .unwrap_or_else(|| Self::full_null("literal", &inner_dtype, size)),
                        )
                    })
                    .unzip();

                let validity = arrow2::bitmap::Bitmap::from(validity);

                let flat_child = Self::concat(&data.iter().collect::<Vec<_>>())?;

                let physical =
                    FixedSizeListArray::new(field.to_physical(), flat_child, Some(validity));

                EmbeddingArray::new(field, physical).into_series()
            }
            DataType::Map {
                key: key_dtype,
                value: value_dtype,
            } => {
                let data = values
                    .into_iter()
                    .map(|v| {
                        unwrap_inner!(v, Literal::Map { keys, values } => (keys, values))
                            .map(|(k, v)| {
                                Ok(StructArray::new(
                                    field
                                        .to_physical()
                                        .to_exploded_field()
                                        .expect("Expected physical type of map to be list"),
                                    vec![
                                        k.cast(&key_dtype)?.rename("key"),
                                        v.cast(&value_dtype)?.rename("value"),
                                    ],
                                    None,
                                )
                                .into_series())
                            })
                            .transpose()
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                let physical = ListArray::try_from(("literal", data.as_slice()))?;

                MapArray::new(field, physical).into_series()
            }
            DataType::Image(image_mode) => {
                let data = values
                    .map(|v| unwrap_inner!(v, Image).map(|img| CowImage::from(img.0)))
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
        };
        if let Some(e) = err {
            Err(DaftError::ComputeError(e))
        } else {
            Ok(s)
        }
    }

    /// Converts a vec of literals into a Series.
    ///
    /// Literals must all be the same type or null, this function does not do any casting or coercion.
    /// If that is desired, you should handle it for each literal before converting it into a series.
    pub fn from_literals(values: Vec<Literal>, dtype: Option<DataType>) -> DaftResult<Self> {
        let dtype = match dtype {
            Some(dtype) => dtype,
            None => values.iter().try_fold(DataType::Null, |acc, v| {
                let dtype = v.get_type();
                combine_lit_types(&acc, &dtype).ok_or_else(|| {
                    DaftError::ValueError(format!(
                        "All literals must have the same data type or null. Found: {} vs {}",
                        acc, dtype
                    ))
                })
            })?,
        };
        Self::from_literals_iter(
            values.into_iter().map(DaftResult::Ok),
            dtype,
            OnError::Raise,
        )
    }
}

impl From<Literal> for Series {
    fn from(value: Literal) -> Self {
        let dtype = Some(value.get_type());
        Self::from_literals(vec![value], dtype)
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
    #[case::list(vec![
        Literal::List(Series::empty("literal", &DataType::Int32)),
        Literal::List(series![1, 2, 3]),
        Literal::List(Series::empty("literal", &DataType::Null))
    ])]
    #[case::struct_(vec![
        Literal::Struct(indexmap!{"a".to_string() => Literal::Boolean(true)}),
        Literal::Struct(indexmap!{"a".to_string() => Literal::Boolean(false)}),
        Literal::Struct(indexmap!{"a".to_string() => Literal::Null}),
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
        Literal::Map { keys: Series::empty("literal", &DataType::Null), values: Series::empty("literal", &DataType::Null) },
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
        let series = Series::from_literals(expected.clone(), None).unwrap();
        let actual = series.to_literals().collect::<Vec<_>>();

        assert_eq!(expected, actual)
    }

    #[test]
    fn test_literals_to_series_mismatched() {
        let values = vec![Literal::UInt64(1), Literal::Utf8("test".to_string())];
        let actual = Series::from_literals(values, None);
        assert!(actual.is_err());
    }
}
