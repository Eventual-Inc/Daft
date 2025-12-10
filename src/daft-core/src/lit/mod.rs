mod conversions;
mod deserializer;
#[cfg(feature = "python")]
pub(crate) mod python;
use std::{
    fmt::{Display, Formatter, Result},
    hash::{Hash, Hasher},
    io::{self, Write},
};

use common_display::table_display::StrValue;
use common_error::{DaftError, DaftResult, ensure};
use common_hashable_float_wrapper::FloatWrapper;
use common_image::{CowImage, Image};
#[cfg(feature = "python")]
use common_py_serde::PyObjectWrapper;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    datatypes::IntervalValue,
    file::FileReference,
    prelude::*,
    series::from_lit::combine_lit_types,
    utils::display::{
        display_date32, display_decimal128, display_duration, display_series_in_literal,
        display_time64, display_timestamp,
    },
};

/// Stores a literal value for queries and computations.
/// We only need to support the limited types below since those are the types that we would get from python.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Literal {
    Null,
    /// A binary true or false.
    Boolean(bool),
    /// A UTF8 encoded string type.
    Utf8(String),
    /// A raw binary array
    Binary(Vec<u8>),
    /// A 8-bit signed integer number.
    Int8(i8),
    /// A 8-bit unsigned integer number.
    UInt8(u8),
    /// A 16-bit signed integer number.
    Int16(i16),
    /// A 16-bit unsigned integer number.
    UInt16(u16),
    /// A 32-bit signed integer number.
    Int32(i32),
    /// A 32-bit unsigned integer number.
    UInt32(u32),
    /// A 64-bit signed integer number.
    Int64(i64),
    /// A 64-bit unsigned integer number.
    UInt64(u64),
    /// A [`i64`] representing a timestamp measured in [`TimeUnit`] with an optional timezone.
    ///
    /// Time is measured as a Unix epoch, counting the seconds from
    /// 00:00:00.000 on 1 January 1970, excluding leap seconds,
    /// as a 64-bit signed integer.
    ///
    /// The time zone is a string indicating the name of a time zone, one of:
    ///
    /// * As used in the Olson time zone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York"
    /// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
    ///
    /// When the timezone is not specified, the timestamp is considered to have no timezone
    /// and is represented _as is_
    Timestamp(i64, TimeUnit, Option<String>),
    /// An [`i32`] representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date(i32),
    /// An [`i64`] representing a time in microseconds or nanoseconds since midnight.
    Time(i64, TimeUnit),
    /// An [`i64`] representing a measure of elapsed time. This elapsed time is a physical duration (i.e. 1s as defined in S.I.)
    Duration(i64, TimeUnit),
    /// Interval: relative elapsed time measured in (months, days, nanoseconds)
    Interval(IntervalValue),
    /// A 32-bit floating point number.
    Float32(f32),
    /// A 64-bit floating point number.
    Float64(f64),
    /// An [`i128`] representing a decimal number with the provided precision and scale.
    Decimal(i128, u8, i8),
    /// A list literal.
    List(Series),
    /// Python object.
    #[cfg(feature = "python")]
    Python(PyObjectWrapper),
    /// TODO chore: audit struct literal vs. struct expression support.
    Struct(IndexMap<String, Literal>),
    File(FileReference),
    /// A tensor
    Tensor {
        data: Series,
        shape: Vec<u64>,
    },
    /// A sparse tensor (values, indices, shape, indices_offset)
    SparseTensor {
        values: Series,
        indices: Series,
        shape: Vec<u64>,
        indices_offset: bool,
    },
    /// Embedding data and size
    Embedding(Series),
    /// Map keys and values
    Map {
        keys: Series,
        values: Series,
    },
    // An image buffer
    Image(Image),
    // Extension type, stored as a single-element series
    Extension(Series),
}

impl Eq for Literal {}

impl Hash for Literal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 1.hash(state),
            Self::Boolean(bool) => bool.hash(state),
            Self::Utf8(s) => s.hash(state),
            Self::Binary(arr) => arr.hash(state),
            Self::Int8(n) => n.hash(state),
            Self::UInt8(n) => n.hash(state),
            Self::Int16(n) => n.hash(state),
            Self::UInt16(n) => n.hash(state),
            Self::Int32(n) => n.hash(state),
            Self::UInt32(n) => n.hash(state),
            Self::Int64(n) => n.hash(state),
            Self::UInt64(n) => n.hash(state),
            Self::Date(n) => n.hash(state),
            Self::Time(n, tu) => {
                n.hash(state);
                tu.hash(state);
            }
            Self::Timestamp(n, tu, tz) => {
                n.hash(state);
                tu.hash(state);
                tz.hash(state);
            }
            Self::Duration(n, tu) => {
                n.hash(state);
                tu.hash(state);
            }
            Self::Interval(n) => {
                n.hash(state);
            }
            Self::Float32(n) => FloatWrapper(*n).hash(state),
            Self::Float64(n) => FloatWrapper(*n).hash(state),
            Self::Decimal(n, precision, scale) => {
                n.hash(state);
                precision.hash(state);
                scale.hash(state);
            }
            Self::List(series) => {
                Hash::hash(series, state);
            }
            #[cfg(feature = "python")]
            Self::Python(py_obj) => py_obj.hash(state),
            Self::Struct(entries) => {
                entries.iter().for_each(|(v, f)| {
                    v.hash(state);
                    f.hash(state);
                });
            }
            Self::File(file) => file.hash(state),
            Self::Tensor { data, shape } => {
                Hash::hash(data, state);
                shape.hash(state);
            }
            Self::SparseTensor {
                values,
                indices,
                shape,
                indices_offset,
            } => {
                Hash::hash(values, state);
                Hash::hash(indices, state);
                shape.hash(state);
                indices_offset.hash(state);
            }
            Self::Embedding(data) => {
                Hash::hash(data, state);
            }
            Self::Map { keys, values } => {
                Hash::hash(keys, state);
                Hash::hash(values, state);
            }
            Self::Image(image_buffer_wrapper) => {
                image_buffer_wrapper.hash(state);
            }
            Self::Extension(series) => Hash::hash(series, state),
        }
    }
}

impl Display for Literal {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            Self::Null => write!(f, "Null"),
            Self::Boolean(val) => write!(f, "{val}"),
            Self::Utf8(val) => write!(f, "\"{val}\""),
            Self::Binary(val) => write!(f, "Binary[{}]", val.len()),
            Self::Int8(val) => write!(f, "{val}"),
            Self::UInt8(val) => write!(f, "{val}"),
            Self::Int16(val) => write!(f, "{val}"),
            Self::UInt16(val) => write!(f, "{val}"),
            Self::Int32(val) => write!(f, "{val}"),
            Self::UInt32(val) => write!(f, "{val}"),
            Self::Int64(val) => write!(f, "{val}"),
            Self::UInt64(val) => write!(f, "{val}"),
            Self::Date(val) => write!(f, "{}", display_date32(*val)),
            Self::Time(val, tu) => write!(f, "{}", display_time64(*val, tu)),
            Self::Timestamp(val, tu, tz) => write!(f, "{}", display_timestamp(*val, tu, tz)),
            Self::Duration(val, tu) => write!(f, "{}", display_duration(*val, tu)),
            Self::Float32(val) => write!(f, "{val:.1}"),
            Self::Float64(val) => write!(f, "{val:.1}"),
            Self::Decimal(val, precision, scale) => {
                write!(f, "{}", display_decimal128(*val, *precision, *scale))
            }
            Self::Interval(value) => write!(f, "{value}"),
            Self::List(series) => write!(f, "{}", display_series_in_literal(series)),
            #[cfg(feature = "python")]
            Self::Python(pyobj) => write!(f, "PyObject({})", {
                use pyo3::prelude::*;
                Python::attach(|py| pyobj.0.call_method0(py, pyo3::intern!(py, "__str__"))).unwrap()
            }),
            Self::Struct(entries) => {
                write!(f, "Struct(")?;
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, ")")
            }
            Self::File(file) => write!(f, "{file}"),
            Self::Tensor { data, shape } => {
                write!(
                    f,
                    "Tensor({}, shape=[{:?}]",
                    display_series_in_literal(data),
                    shape
                )
            }
            Self::SparseTensor {
                values,
                indices,
                shape,
                indices_offset,
            } => {
                write!(
                    f,
                    "SparseTensor({}, indices=[{:?}], shape=[{:?}], indices_offset={}",
                    display_series_in_literal(values),
                    indices,
                    shape,
                    indices_offset
                )
            }
            Self::Embedding(data) => {
                write!(f, "Embedding{}", display_series_in_literal(data))
            }
            Self::Map { keys, values } => {
                assert_eq!(
                    keys.len(),
                    values.len(),
                    "Key and value counts should be equal in map literal"
                );

                write!(
                    f,
                    "Map({})",
                    (0..keys.len())
                        .map(|i| { format!("{}: {}", keys.str_value(i), values.str_value(i)) })
                        .join(", ")
                )
            }
            Self::Image(image_buffer_wrapper) => write!(f, "Image({image_buffer_wrapper:?})"),
            Self::Extension(series) => write!(f, "Extension(\n{}\n)", series),
        }
    }
}

impl Literal {
    /// Get the data type of a literal value.
    ///
    /// When changing this, make sure it matches the datatypes covered in `TryFrom<Vec<Literal>> for Series`
    pub fn get_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Utf8(_) => DataType::Utf8,
            Self::Binary(_) => DataType::Binary,
            Self::Int8(_) => DataType::Int8,
            Self::UInt8(_) => DataType::UInt8,
            Self::Int16(_) => DataType::Int16,
            Self::UInt16(_) => DataType::UInt16,
            Self::Int32(_) => DataType::Int32,
            Self::UInt32(_) => DataType::UInt32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Date(_) => DataType::Date,
            Self::Time(_, tu) => DataType::Time(*tu),
            Self::Timestamp(_, tu, tz) => DataType::Timestamp(*tu, tz.clone()),
            Self::Duration(_, tu) => DataType::Duration(*tu),
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Decimal(_, precision, scale) => {
                DataType::Decimal128(*precision as usize, *scale as usize)
            }
            Self::Interval(_) => DataType::Interval,
            Self::List(series) => DataType::List(Box::new(series.data_type().clone())),
            #[cfg(feature = "python")]
            Self::Python(_) => DataType::Python,
            Self::Struct(entries) => DataType::Struct(
                entries
                    .iter()
                    .map(|(k, v)| Field::new(k, v.get_type()))
                    .collect(),
            ),
            Self::File(f) => DataType::File(f.media_type),
            Self::Tensor { data, .. } => DataType::Tensor(Box::new(data.data_type().clone())),
            Self::SparseTensor {
                values,
                indices_offset,
                ..
            } => DataType::SparseTensor(Box::new(values.data_type().clone()), *indices_offset),
            Self::Embedding(data) => {
                DataType::Embedding(Box::new(data.data_type().clone()), data.len())
            }
            Self::Map { keys, values } => DataType::Map {
                key: Box::new(keys.data_type().clone()),
                value: Box::new(values.data_type().clone()),
            },
            Self::Image(image_buffer_wrapper) => {
                DataType::Image(Some(CowImage::from(&image_buffer_wrapper.0).mode()))
            }
            Self::Extension(series) => series.data_type().clone(),
        }
    }

    pub fn neg(&self) -> DaftResult<Self> {
        match self {
            Self::Int8(v) => Ok(Self::Int8(-v)),
            Self::Int16(v) => Ok(Self::Int16(-v)),
            Self::Int32(v) => Ok(Self::Int32(-v)),
            Self::Int64(v) => Ok(Self::Int64(-v)),
            Self::UInt8(v) => {
                if *v > 0 {
                    Ok(Self::Int16(-(*v as i16)))
                } else {
                    Ok(self.clone())
                }
            }
            Self::UInt16(v) => {
                if *v > 0 {
                    Ok(Self::Int32(-(*v as i32)))
                } else {
                    Ok(self.clone())
                }
            }
            Self::UInt32(v) => {
                if *v > 0 {
                    Ok(Self::Int64(-(*v as i64)))
                } else {
                    Ok(self.clone())
                }
            }
            Self::UInt64(v) => {
                if *v > 0 {
                    if *v > i64::MAX as u64 {
                        return Err(DaftError::ValueError(format!(
                            "Cannot negate UInt64 value {} as it exceeds i64::MAX",
                            v
                        )));
                    }

                    Ok(Self::Int64(-(*v as i64)))
                } else {
                    Ok(self.clone())
                }
            }
            Self::Float32(v) => Ok(Self::Float32(-v)),
            Self::Float64(v) => Ok(Self::Float64(-v)),
            Self::Decimal(v, precision, scale) => Ok(Self::Decimal(-v, *precision, *scale)),
            Self::Duration(v, tu) => Ok(Self::Duration(-v, *tu)),
            Self::Interval(v) => Ok(Self::Interval(-v)),
            _ => Err(DaftError::ValueError(format!(
                "Cannot negate literal for type: {}",
                self.get_type()
            ))),
        }
    }

    pub fn display_sql<W: Write>(&self, buffer: &mut W) -> io::Result<()> {
        let display_sql_err = Err(io::Error::other("Unsupported literal for SQL translation"));
        match self {
            Self::Null => write!(buffer, "NULL"),
            Self::Boolean(val) => write!(buffer, "{}", val),
            Self::Int8(val) => write!(buffer, "{}", val),
            Self::UInt8(val) => write!(buffer, "{}", val),
            Self::Int16(val) => write!(buffer, "{}", val),
            Self::UInt16(val) => write!(buffer, "{}", val),
            Self::Int32(val) => write!(buffer, "{}", val),
            Self::UInt32(val) => write!(buffer, "{}", val),
            Self::Int64(val) => write!(buffer, "{}", val),
            Self::UInt64(val) => write!(buffer, "{}", val),
            Self::Float32(val) => write!(buffer, "{}", val),
            Self::Float64(val) => write!(buffer, "{}", val),
            Self::Utf8(val) => write!(buffer, "'{}'", val),
            Self::Date(val) => write!(buffer, "DATE '{}'", display_date32(*val)),
            Self::Timestamp(val, tu, tz) => write!(
                buffer,
                "TIMESTAMP '{}'",
                display_timestamp(*val, tu, tz).replace('T', " ")
            ),
            Self::Decimal(..)
            | Self::List(..)
            | Self::Time(..)
            | Self::Binary(..)
            | Self::Duration(..)
            | Self::Interval(..)
            | Self::Struct(..)
            | Self::Tensor { .. }
            | Self::SparseTensor { .. }
            | Self::Embedding { .. }
            | Self::Map { .. }
            | Self::Image(_)
            | Self::Extension(_) => display_sql_err,
            #[cfg(feature = "python")]
            Self::Python(..) => display_sql_err,
            Self::File(_) => display_sql_err,
        }
    }

    /// If the liter is a boolean, return it. Otherwise, return None.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// If the literal is a string, return it. Otherwise, return None.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Utf8(s) => Some(s),
            _ => None,
        }
    }

    /// If the literal is `Binary`, return it. Otherwise, return None.
    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) => Some(b),
            _ => None,
        }
    }

    /// If the literal is `Int8`, return it. Otherwise, return None.
    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Self::Int8(i) => Some(*i),
            _ => None,
        }
    }

    /// If the literal is `UInt8`, return it. Otherwise, return None.
    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Self::UInt8(i) => Some(*i),
            _ => None,
        }
    }

    /// If the literal is `Int16`, return it. Otherwise, return None.
    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Self::Int16(i) => Some(*i),
            _ => None,
        }
    }

    /// If the literal is `UInt16`, return it. Otherwise, return None.
    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Self::UInt16(i) => Some(*i),
            _ => None,
        }
    }

    /// If the literal is `Int32`, return it. Otherwise, return None.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Self::Int32(i) => Some(*i),
            _ => None,
        }
    }

    /// If the literal is `UInt32`, return it. Otherwise, return None.
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Self::UInt32(i) => Some(*i),
            _ => None,
        }
    }

    /// If the literal is `Int64`, return it. Otherwise, return None.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(i) => Some(*i),
            _ => None,
        }
    }

    /// If the literal is `UInt64`, return it. Otherwise, return None.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::UInt64(i) => Some(*i),
            _ => None,
        }
    }
    pub fn try_as_usize(&self) -> DaftResult<Option<usize>> {
        match self {
            Self::Int8(i) => usize::try_from(*i).map(Some),
            Self::Int16(i) => usize::try_from(*i).map(Some),
            Self::Int32(i) => usize::try_from(*i).map(Some),
            Self::Int64(i) => usize::try_from(*i).map(Some),
            Self::UInt32(i) => Ok(Some(*i as usize)),
            Self::UInt64(i) => Ok(Some(*i as usize)),
            _ => Ok(None),
        }
        .map_err(|e| DaftError::ValueError(format!("Failed to convert literal to usize: {}", e)))
    }

    /// If the literal is `Float32`, return it. Otherwise, return None.
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::Float32(f) => Some(*f),
            _ => None,
        }
    }

    /// If the literal is `Float64`, return it. Otherwise, return None.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(f) => Some(*f),
            _ => None,
        }
    }

    /// If the literal is a struct, return the reference to its map. Otherwise, return None.
    pub fn as_struct(&self) -> Option<&IndexMap<String, Self>> {
        match self {
            Self::Struct(map) => Some(map),
            _ => None,
        }
    }

    pub fn try_from_single_value_series(s: &Series) -> DaftResult<Self> {
        ensure!(s.len() == 1, ValueError: "expected a scalar value");
        Ok(s.get_lit(0))
    }

    // =================
    //  Factory Methods
    // =================

    pub fn new_struct<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (String, Self)>,
    {
        // A "struct" literal is a strange concept, and only makes
        // sense that it predates the struct expression. The literals
        // tell us the type, so need to give before construction.
        Self::Struct(IndexMap::from_iter(iter))
    }

    /// Cast the literal to a data type.
    ///
    /// This method is lossy, AKA it is not guaranteed that `lit.cast(dtype).get_type() == dtype`.
    /// This is because null literals always have the null data type.
    pub fn cast(self, dtype: &DataType) -> DaftResult<Self> {
        if &self.get_type() == dtype
            || (combine_lit_types(&self.get_type(), dtype).as_ref() == Some(dtype))
        {
            Ok(self)
        } else {
            Series::from(self)
                .cast(dtype)
                .and_then(|s| Self::try_from_single_value_series(&s))
        }
    }
}

pub trait FromLiteral: Sized {
    fn try_from_literal(lit: &Literal) -> DaftResult<Self>;
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;

    use super::{FromLiteral, Literal};

    #[test]
    fn test_roundtrip() -> DaftResult<()> {
        fn roundtrip<V: Into<Literal> + FromLiteral + Clone + std::fmt::Debug + PartialEq>(
            value: V,
        ) -> DaftResult<()> {
            let lit = value.clone().into();
            let deserialized = V::try_from_literal(&lit).expect("Failed to deserialize Literal");
            assert_eq!(value, deserialized);
            Ok(())
        }

        roundtrip(1i8)?;
        roundtrip(1i16)?;
        roundtrip(1i32)?;
        roundtrip(1i64)?;
        roundtrip(1u8)?;
        roundtrip(1u16)?;
        roundtrip(1u32)?;
        roundtrip(1u64)?;
        roundtrip(1f64)?;
        roundtrip("test".to_string())?;

        Ok(())
    }
}
