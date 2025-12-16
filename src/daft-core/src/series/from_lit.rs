use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_image::CowImage;
use daft_arrow::{
    array::{
        MutableArray, MutableBinaryArray, MutableBooleanArray, MutablePrimitiveArray,
        MutableUtf8Array,
    },
    trusted_len::TrustedLen,
    types::{NativeType, months_days_ns},
};
use indexmap::IndexMap;
use itertools::Itertools;

use crate::{
    array::ops::image::image_array_from_img_buffers,
    datatypes::FileArray,
    file::{MediaTypeAudio, MediaTypeUnknown, MediaTypeVideo},
    prelude::*,
};

/// Downcasts a datatype to one that's compatible with literals.
/// example:
/// ```rust,no_run
/// // Literal's dont support fixed size binary
/// // so it gets downcast to `DataType::Binary`
/// let dtype = DataType::FixedSizeBinary(10);
/// let downcasted = dtype.downcast_to_lit_compatible();
/// assert_eq!(downcasted, DataType::Binary);
/// ```
fn downcast_to_lit_compatible(dtype: DataType) -> DaftResult<DataType> {
    Ok(match dtype {
        DataType::FixedSizeBinary(..) => DataType::Binary,
        DataType::FixedSizeList(dtype, _) => DataType::List(dtype),
        DataType::FixedShapeImage(mode, _, _) => DataType::Image(Some(mode)),
        DataType::FixedShapeTensor(dtype, _) => DataType::Tensor(dtype),
        DataType::FixedShapeSparseTensor(dtype, _, b) => DataType::SparseTensor(dtype, b),
        DataType::Unknown | DataType::Extension(..) => {
            return Err(DaftError::TypeError(format!(
                "DataType {dtype:?} does not have a `downcast_to_lit_compatible` property",
            )));
        }
        other => other,
    })
}

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

/// Creates a series from an iterator of `Result<Literal>`.
/// It also captures any errors occurred during iteration
/// It returns a Series, along with an IndexMap<row_idx, error>
pub fn series_from_literals_iter<I: ExactSizeIterator<Item = DaftResult<Literal>> + TrustedLen>(
    values: I,
    dtype: Option<DataType>,
) -> DaftResult<Series> {
    let (dtype, values) = match dtype {
        Some(dtype) => (dtype, values),
        None => {
            let values = values.collect::<DaftResult<Vec<_>>>()?;

            let dtype = values.iter().try_fold(DataType::Null, |acc, v| {
                let dtype = v.get_type();
                combine_lit_types(&acc, &dtype).ok_or_else(|| {
                    DaftError::ValueError(format!(
                        "All literals must have the same data type or null. Found: {} vs {}",
                        acc, dtype
                    ))
                })
            })?;
            return series_from_literals_iter(values.into_iter().map(DaftResult::Ok), Some(dtype));
        }
    };
    let len = values.len();
    if len == 0 {
        return Ok(Series::empty("literal", &dtype));
    }
    let downcasted = downcast_to_lit_compatible(dtype.clone())?;

    let mut errs: IndexMap<usize, String> = IndexMap::new();
    let values = values.enumerate();

    let field = Field::new("literal", downcasted.clone());

    macro_rules! unwrap_inner {
        ($expr:expr, $idx:expr, $variant:ident) => {
            match $expr {
                Ok(Literal::$variant(val)) => Some(val),
                Ok(Literal::Null) => None,
                Err(e) =>  {
                    errs.insert($idx, e.to_string());
                    None

                },
                Ok(other) => {
                    let ty = other.get_type();
                    errs.insert($idx, format!(
                        "All literals must have the same data type or null. Found: {ty} vs {dtype}"
                    ));
                    None

                }
            }
        };

        ($expr:expr, $idx:expr, $literal:pat => $value:expr) => {
            match $expr {
                Ok($literal) => Some($value),
                Ok(Literal::Null) => None,
                Err(e) =>{
                    errs.insert($idx, e.to_string());
                    None
                },
                Ok(other) => {
                    let ty = other.get_type();
                    errs.insert($idx, format!(
                        "All literals must have the same data type or null. Found: {ty} vs {dtype}"
                    ));
                    None
                }
            }
        };
    }

    #[inline(always)]
    fn from_mutable_primitive<T: NativeType, I: Iterator<Item = (usize, DaftResult<Literal>)>>(
        values: I,
        values_len: usize,
        field: Field,
        mut f: impl FnMut(DaftResult<Literal>, usize) -> Option<T>,
    ) -> DaftResult<Series> {
        let mut arr = MutablePrimitiveArray::<T>::with_capacity(values_len);

        for (i, value) in values {
            let value = f(value, i);

            arr.push(value);
        }

        Series::from_arrow(Arc::new(field), arr.as_box())
    }

    let s = match downcasted.clone() {
        DataType::Null => NullArray::full_null("literal", &dtype, len).into_series(),
        DataType::Boolean => {
            let mut arr = MutableBooleanArray::with_capacity(len);

            for (i, value) in values {
                let value = unwrap_inner!(value, i, Boolean);
                arr.push(value);
            }
            Series::from_arrow(Arc::new(field), arr.as_box())?
        }
        DataType::Utf8 => {
            let mut arr = MutableUtf8Array::<i64>::with_capacity(len);

            for (i, value) in values {
                let value = unwrap_inner!(value, i, Utf8);

                arr.push(value);
            }
            Series::from_arrow(Arc::new(field), arr.as_box())?
        }
        DataType::Binary => {
            let mut arr = MutableBinaryArray::<i64>::with_capacity(len);

            for (i, value) in values {
                let value = unwrap_inner!(value, i, Binary);
                arr.push(value);
            }
            Series::from_arrow(Arc::new(field), arr.as_box())?
        }
        DataType::Int8 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, Int8))?
        }

        DataType::UInt8 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, UInt8))?
        }
        DataType::Int16 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, Int16))?
        }
        DataType::UInt16 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, UInt16))?
        }
        DataType::Int32 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, Int32))?
        }
        DataType::UInt32 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, UInt32))?
        }
        DataType::Int64 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, Int64))?
        }
        DataType::UInt64 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, UInt64))?
        }
        DataType::Float32 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, Float32))?
        }
        DataType::Float64 => {
            from_mutable_primitive(values, len, field, |v, i| unwrap_inner!(v, i, Float64))?
        }
        DataType::Interval => from_mutable_primitive(
            values,
            len,
            field,
            |v, i| unwrap_inner!(v, i, Literal::Interval(v) => months_days_ns::from(v)),
        )?,

        DataType::Decimal128 { .. } => Decimal128Array::from_iter(
            field,
            values.map(|(i, lit)| unwrap_inner!(lit, i, Literal::Decimal(v, ..) => v)),
        )
        .into_series(),
        DataType::Timestamp(_, _) => {
            let mut arr = MutablePrimitiveArray::<i64>::with_capacity(len);

            for (i, value) in values {
                let value = unwrap_inner!(value, i, Literal::Timestamp(ts, ..) => ts);
                arr.push(value);
            }
            let physical = Int64Array::from_arrow(
                Arc::new(Field::new("literal", DataType::Int64)),
                arr.as_box(),
            )?;
            TimestampArray::new(field, physical).into_series()
        }
        DataType::Date => {
            let mut arr = MutablePrimitiveArray::<i32>::with_capacity(len);

            for (i, value) in values {
                let value = unwrap_inner!(value, i, Literal::Date(d) => d);
                arr.push(value);
            }
            let physical = Int32Array::from_arrow(
                Arc::new(Field::new("literal", DataType::Int32)),
                arr.as_box(),
            )?;

            DateArray::new(field, physical).into_series()
        }
        DataType::Time(_) => {
            let data = values.map(|(i, lit)| unwrap_inner!(lit, i, Literal::Time(t, ..) => t));
            let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);

            TimeArray::new(field, physical).into_series()
        }
        DataType::Duration(_) => {
            let data = values.map(|(i, lit)| unwrap_inner!(lit, i, Literal::Duration(v, ..) => v));
            let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);

            DurationArray::new(field, physical).into_series()
        }
        DataType::List(ref child_dtype) => {
            let data = values
                .map(|(i, lit)| {
                    unwrap_inner!(lit, i, List)
                        .map(|s| s.cast(child_dtype))
                        .transpose()
                })
                .collect::<DaftResult<Vec<_>>>()?;
            ListArray::try_from(("literal", data.as_slice()))?.into_series()
        }
        DataType::Struct(ref fields) => {
            let values = values.collect::<Vec<_>>();
            let children = fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let child_values = values
                        .iter()
                        .map(|(j, v)| match unwrap_inner!(v, *j, Struct) {
                            Some(v) => v[i].clone(),
                            None => Literal::Null,
                        })
                        .collect::<Vec<_>>();

                    Ok(series_from_literals_iter(
                        child_values.into_iter().map(Ok),
                        Some(f.dtype.clone()),
                    )?
                    .rename(&f.name))
                })
                .collect::<DaftResult<_>>()?;

            let validity = daft_arrow::buffer::NullBuffer::from_iter(
                values
                    .iter()
                    .map(|(_, v)| v.as_ref().is_ok_and(|v| v != &Literal::Null)),
            );

            StructArray::new(field, children, Some(validity)).into_series()
        }

        #[cfg(feature = "python")]
        DataType::Python => {
            let iter = values.map(|(i, lit)| unwrap_inner!(lit, i, Python).map(|pyobj| pyobj.0));

            PythonArray::from_iter("literal", iter).into_series()
        }
        DataType::File(media_type) => {
            let iter = values
                .map(|(i, lit)| unwrap_inner!(lit, i, File))
                .map(DaftResult::Ok);
            match media_type {
                daft_schema::media_type::MediaType::Unknown => {
                    FileArray::<MediaTypeUnknown>::new_from_file_references("literal", iter)?
                        .into_series()
                }
                daft_schema::media_type::MediaType::Video => {
                    FileArray::<MediaTypeVideo>::new_from_file_references("literal", iter)?
                        .into_series()
                }
                daft_schema::media_type::MediaType::Audio => {
                    FileArray::<MediaTypeAudio>::new_from_file_references("literal", iter)?
                        .into_series()
                }
            }
        }
        DataType::Tensor(_) => {
            let (data, shapes) = values
                .map(|(i, v)| {
                    unwrap_inner!(v, i, Literal::Tensor { data, shape } => (data, shape)).unzip()
                })
                .collect::<(Vec<_>, Vec<_>)>();

            let data_array = ListArray::try_from(("data", data.as_slice()))?.into_series();
            let shape_array = ListArray::from_vec("shape", shapes).into_series();

            let validity = data_array.validity().cloned();
            let physical =
                StructArray::new(field.to_physical(), vec![data_array, shape_array], validity);

            TensorArray::new(field, physical).into_series()
        }
        DataType::SparseTensor(..) => {
            let (values, indices, shapes) = values

                .map(|(i, v)| {
                    match unwrap_inner!(v, i, Literal::SparseTensor { values, indices, shape, .. } => (values, indices, shape)) {
                        Some((v, i, s)) => (Some(v), Some(i), Some(s)),
                        None => (None, None, None)
                    }
                })
                .collect::<(Vec<_>, Vec<_>, Vec<_>)>();

            let values_array = ListArray::try_from(("values", values.as_slice()))?.into_series();
            let indices_array = ListArray::try_from(("indices", indices.as_slice()))?.into_series();
            let shape_array = ListArray::from_vec("shape", shapes).into_series();

            let validity = values_array.validity().cloned();
            let physical = StructArray::new(
                field.to_physical(),
                vec![values_array, indices_array, shape_array],
                validity,
            );

            SparseTensorArray::new(field, physical).into_series()
        }
        DataType::Embedding(ref inner_dtype, ref size) => {
            let (validity, data): (Vec<_>, Vec<_>) = values
                .map(|(i, v)| {
                    (
                        v.as_ref().is_ok_and(|v| v != &Literal::Null),
                        unwrap_inner!(v, i, Embedding)
                            .unwrap_or_else(|| Series::full_null("literal", inner_dtype, *size)),
                    )
                })
                .unzip();

            let validity = daft_arrow::buffer::NullBuffer::from(validity);

            let flat_child = Series::concat(&data.iter().collect::<Vec<_>>())?;

            let physical = FixedSizeListArray::new(field.to_physical(), flat_child, Some(validity));

            EmbeddingArray::new(field, physical).into_series()
        }
        DataType::Map {
            key: ref key_dtype,
            value: ref value_dtype,
        } => {
            let data = values
                .map(|(i, v)| {
                    unwrap_inner!(v, i, Literal::Map { keys, values } => (keys, values))
                        .map(|(k, v)| {
                            Ok(StructArray::new(
                                field
                                    .to_physical()
                                    .to_exploded_field()
                                    .expect("Expected physical type of map to be list"),
                                vec![
                                    k.cast(key_dtype)?.rename("key"),
                                    v.cast(value_dtype)?.rename("value"),
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
            let data =
                values.map(|(i, v)| unwrap_inner!(v, i, Image).map(|img| CowImage::from(img.0)));
            struct ExactSizeWrapper<I> {
                iter: I,
                len: usize,
            }
            impl<I: Iterator> Iterator for ExactSizeWrapper<I> {
                type Item = I::Item;

                fn next(&mut self) -> Option<Self::Item> {
                    if self.len == 0 {
                        return None;
                    }
                    self.len -= 1;
                    self.iter.next()
                }

                fn size_hint(&self) -> (usize, Option<usize>) {
                    (self.len, Some(self.len))
                }
            }

            impl<I: Iterator> ExactSizeIterator for ExactSizeWrapper<I> {}
            let iter = ExactSizeWrapper { iter: data, len };

            image_array_from_img_buffers("literal", iter, image_mode)?.into_series()
        }

        DataType::FixedSizeBinary(..)
        | DataType::FixedSizeList(..)
        | DataType::Extension(..)
        | DataType::FixedShapeImage(..)
        | DataType::FixedShapeTensor(..)
        | DataType::FixedShapeSparseTensor(..)
        | DataType::Unknown => unreachable!("Literal should never have data type: {dtype}"),
    };

    let s = if downcasted != dtype {
        s.cast(&dtype)?
    } else {
        s
    };

    if errs.is_empty() {
        Ok(s)
    } else {
        let errs = errs
            .into_iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .join("\n");
        Err(common_error::DaftError::ComputeError(format!(
            "Error processing some rows:\n{errs}"
        )))
    }
}

impl Series {
    /// Converts a vec of literals into a Series.
    ///
    /// Literals must all be the same type or null, this function does not do any casting or coercion.
    /// If that is desired, you should handle it for each literal before converting it into a series.
    pub fn from_literals(values: Vec<Literal>) -> DaftResult<Self> {
        series_from_literals_iter(values.into_iter().map(DaftResult::Ok), None)
    }
}

impl From<Literal> for Series {
    fn from(value: Literal) -> Self {
        Self::from_literals(vec![value])
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
        let series = Series::from_literals(expected.clone()).unwrap();
        let actual = series.to_literals().collect::<Vec<_>>();

        assert_eq!(expected, actual)
    }

    #[test]
    fn test_literals_to_series_mismatched() {
        let values = vec![Literal::UInt64(1), Literal::Utf8("test".to_string())];
        let actual = Series::from_literals(values);
        assert!(actual.is_err());
    }
}
