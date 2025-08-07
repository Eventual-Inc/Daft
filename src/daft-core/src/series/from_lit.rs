use common_error::{DaftError, DaftResult};

use crate::prelude::*;

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
                    Literal::$variant(val, ..) => Some(val),
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
            DataType::Decimal128 { .. } => from_iter_with_field!(Decimal128Array, Decimal),
            DataType::Timestamp(_, _) => {
                let data = values.into_iter().map(|lit| unwrap_inner!(lit, Timestamp));
                let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);
                TimestampArray::new(field, physical).into_series()
            }
            DataType::Date => {
                let data = values.into_iter().map(|lit| unwrap_inner!(lit, Date));
                let physical = Int32Array::from_iter(Field::new("literal", DataType::Int32), data);
                DateArray::new(field, physical).into_series()
            }
            DataType::Time(_) => {
                let data = values.into_iter().map(|lit| unwrap_inner!(lit, Time));
                let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);

                TimeArray::new(field, physical).into_series()
            }
            DataType::Duration(_) => {
                let data = values.into_iter().map(|lit| unwrap_inner!(lit, Time));
                let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);

                DurationArray::new(field, physical).into_series()
            }
            DataType::List(_) => {
                let lengths = values
                    .iter()
                    .map(|v| unwrap_inner!(v, List).map_or(0, Self::len));
                let offsets = arrow2::offset::Offsets::try_from_lengths(lengths)?.into();

                let validity = arrow2::bitmap::Bitmap::from_trusted_len_iter(
                    values.iter().map(|v| *v != Literal::Null),
                );

                let flat_child = Self::concat(
                    &values
                        .iter()
                        .filter_map(|v| unwrap_inner!(v, List))
                        .collect::<Vec<_>>(),
                )?;

                ListArray::new(field, flat_child, offsets, Some(validity)).into_series()
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

                StructArray::new(field, children, None).into_series()
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
            DataType::FixedSizeBinary(..)
            | DataType::FixedSizeList(..)
            | DataType::Map { .. }
            | DataType::Extension(..)
            | DataType::Embedding(..)
            | DataType::Image(..)
            | DataType::FixedShapeImage(..)
            | DataType::Tensor(..)
            | DataType::FixedShapeTensor(..)
            | DataType::SparseTensor(..)
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
    use crate::prelude::*;

    #[test]
    fn test_literals_to_series() {
        let values = vec![Literal::UInt64(1), Literal::UInt64(2), Literal::UInt64(3)];
        let expected = vec![1, 2, 3];
        let expected = UInt64Array::from_values("literal", expected.into_iter());
        let expected = expected.into_series();
        let actual = Series::try_from(values).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_uint16_literals_to_series() {
        let values = vec![Literal::UInt16(1), Literal::UInt16(2), Literal::UInt16(3)];
        let expected = vec![1, 2, 3];
        let expected = UInt16Array::from_values("literal", expected.into_iter());
        let expected = expected.into_series();
        let actual = Series::try_from(values).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_int8_literals_to_series() {
        let values = vec![Literal::Int8(1), Literal::Int8(2), Literal::Int8(3)];
        let expected = vec![1, 2, 3];
        let expected = Int8Array::from_values("literal", expected.into_iter());
        let expected = expected.into_series();
        let actual = Series::try_from(values).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_literals_to_series_null() {
        let values = vec![Literal::Null, Literal::UInt64(2), Literal::UInt64(3)];
        let expected = vec![None, Some(2), Some(3)];
        let expected = UInt64Array::from_iter(
            Field::new("literal", DataType::UInt64),
            expected.into_iter(),
        );
        let expected = expected.into_series();
        let actual = Series::try_from(values).unwrap();
        // Series.eq returns false for nulls
        for (expected, actual) in expected.u64().iter().zip(actual.u64().iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_literals_to_series_mismatched() {
        let values = vec![Literal::UInt64(1), Literal::Utf8("test".to_string())];
        let actual = Series::try_from(values);
        assert!(actual.is_err());
    }
}
