mod conversions;
mod deserializer;
use std::{
    fmt::{Display, Formatter, Result},
    hash::{Hash, Hasher},
    io::{self, Write},
    sync::Arc,
};

use common_error::{ensure, DaftError, DaftResult};
use common_hashable_float_wrapper::FloatWrapper;
use daft_core::{
    datatypes::IntervalValue,
    prelude::*,
    utils::display::{
        display_date32, display_decimal128, display_duration, display_series_literal,
        display_time64, display_timestamp,
    },
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::pyobj_serde::PyObjectWrapper;
use crate::{expr::Expr, ExprRef};

/// Stores a literal value for queries and computations.
/// We only need to support the limited types below since those are the types that we would get from python.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LiteralValue {
    Null,
    /// A binary true or false.
    Boolean(bool),
    /// A UTF8 encoded string type.
    Utf8(String),
    /// A raw binary array
    Binary(Vec<u8>),
    /// A raw binary array
    FixedSizeBinary(Vec<u8>, usize),
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
    /// A 64-bit floating point number.
    Float64(f64),
    /// An [`i128`] representing a decimal number with the provided precision and scale.
    Decimal(i128, u8, i8),
    /// A series literal.
    Series(Series),
    /// Python object.
    #[cfg(feature = "python")]
    Python(PyObjectWrapper),
    /// TODO chore: audit struct literal vs. struct expression support.
    Struct(IndexMap<Field, LiteralValue>),
}

impl Eq for LiteralValue {}

impl Hash for LiteralValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            // Stable hash for Null variant.
            Self::Null => 1.hash(state),
            Self::Boolean(bool) => bool.hash(state),
            Self::Utf8(s) => s.hash(state),
            Self::Binary(arr) => arr.hash(state),
            Self::FixedSizeBinary(arr, size) => {
                arr.hash(state);
                size.hash(state);
            }
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
            // Wrap float64 in hashable newtype.
            Self::Float64(n) => FloatWrapper(*n).hash(state),
            Self::Decimal(n, precision, scale) => {
                n.hash(state);
                precision.hash(state);
                scale.hash(state);
            }
            Self::Series(series) => {
                let hash_result = series.hash(None);
                match hash_result {
                    Ok(hash) => hash.into_iter().for_each(|i| i.hash(state)),
                    Err(..) => panic!("Cannot hash series"),
                }
            }
            #[cfg(feature = "python")]
            Self::Python(py_obj) => py_obj.hash(state),
            Self::Struct(entries) => {
                entries.iter().for_each(|(v, f)| {
                    v.hash(state);
                    f.hash(state);
                });
            }
        }
    }
}

impl Display for LiteralValue {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            Self::Null => write!(f, "Null"),
            Self::Boolean(val) => write!(f, "{val}"),
            Self::Utf8(val) => write!(f, "\"{val}\""),
            Self::Binary(val) => write!(f, "Binary[{}]", val.len()),
            Self::FixedSizeBinary(val, size) => {
                write!(f, "FixedSizeBinary[{}, {}]", val.len(), size)
            }
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
            Self::Float64(val) => write!(f, "{val:.1}"),
            Self::Decimal(val, precision, scale) => {
                write!(f, "{}", display_decimal128(*val, *precision, *scale))
            }
            Self::Interval(value) => write!(f, "{value}"),
            Self::Series(series) => write!(f, "{}", display_series_literal(series)),
            #[cfg(feature = "python")]
            Self::Python(pyobj) => write!(f, "PyObject({})", {
                use pyo3::prelude::*;
                Python::with_gil(|py| pyobj.0.call_method0(py, pyo3::intern!(py, "__str__")))
                    .unwrap()
            }),
            Self::Struct(entries) => {
                write!(f, "Struct(")?;
                for (i, (field, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, v)?;
                }
                write!(f, ")")
            }
        }
    }
}

impl LiteralValue {
    pub fn get_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Utf8(_) => DataType::Utf8,
            Self::Binary(_) => DataType::Binary,
            Self::FixedSizeBinary(_, size) => DataType::FixedSizeBinary(*size),
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
            Self::Float64(_) => DataType::Float64,
            Self::Decimal(_, precision, scale) => {
                DataType::Decimal128(*precision as usize, *scale as usize)
            }
            Self::Interval(_) => DataType::Interval,
            Self::Series(series) => series.data_type().clone(),
            #[cfg(feature = "python")]
            Self::Python(_) => DataType::Python,
            Self::Struct(entries) => DataType::Struct(entries.keys().cloned().collect()),
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
                    Ok(Self::Int64(-(*v as i64)))
                } else {
                    Ok(self.clone())
                }
            }
            Self::Float64(v) => Ok(Self::Float64(-v)),
            Self::Decimal(v, precision, scale) => Ok(Self::Decimal(-v, *precision, *scale)),
            Self::Duration(v, tu) => Ok(Self::Duration(-v, *tu)),
            Self::Interval(v) => Ok(Self::Interval(-v)),
            _ => Err(DaftError::ValueError(format!(
                "Cannot negate literal: {:?}",
                self
            ))),
        }
    }

    pub fn to_series(&self) -> Series {
        match self {
            Self::Null => NullArray::full_null("literal", &DataType::Null, 1).into_series(),
            Self::Boolean(val) => BooleanArray::from(("literal", [*val].as_slice())).into_series(),
            Self::Utf8(val) => {
                Utf8Array::from(("literal", [val.as_str()].as_slice())).into_series()
            }
            Self::Binary(val) => BinaryArray::from(("literal", val.as_slice())).into_series(),
            Self::FixedSizeBinary(val, size) => {
                FixedSizeBinaryArray::from(("literal", val.as_slice(), *size)).into_series()
            }
            Self::Int8(val) => Int8Array::from(("literal", [*val].as_slice())).into_series(),
            Self::UInt8(val) => UInt8Array::from(("literal", [*val].as_slice())).into_series(),
            Self::Int16(val) => Int16Array::from(("literal", [*val].as_slice())).into_series(),
            Self::UInt16(val) => UInt16Array::from(("literal", [*val].as_slice())).into_series(),
            Self::Int32(val) => Int32Array::from(("literal", [*val].as_slice())).into_series(),
            Self::UInt32(val) => UInt32Array::from(("literal", [*val].as_slice())).into_series(),
            Self::Int64(val) => Int64Array::from(("literal", [*val].as_slice())).into_series(),
            Self::UInt64(val) => UInt64Array::from(("literal", [*val].as_slice())).into_series(),

            Self::Date(val) => {
                let physical = Int32Array::from(("literal", [*val].as_slice()));
                DateArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Self::Time(val, ..) => {
                let physical = Int64Array::from(("literal", [*val].as_slice()));
                TimeArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Self::Timestamp(val, ..) => {
                let physical = Int64Array::from(("literal", [*val].as_slice()));
                TimestampArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Self::Duration(val, ..) => {
                let physical = Int64Array::from(("literal", [*val].as_slice()));
                DurationArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Self::Interval(val) => IntervalArray::from_values(
                "literal",
                std::iter::once((val.months, val.days, val.nanoseconds)),
            )
            .into_series(),
            Self::Float64(val) => Float64Array::from(("literal", [*val].as_slice())).into_series(),
            Self::Decimal(val, p, s) => {
                let dtype = DataType::Decimal128(*p as usize, *s as usize);
                let field = Field::new("literal", dtype);
                Decimal128Array::from_values_iter(field, std::iter::once(*val)).into_series()
            }
            Self::Series(series) => series.clone().rename("literal"),
            #[cfg(feature = "python")]
            Self::Python(val) => PythonArray::from(("literal", vec![val.0.clone()])).into_series(),
            Self::Struct(entries) => {
                let struct_dtype = DataType::Struct(entries.keys().cloned().collect());
                let struct_field = Field::new("literal", struct_dtype);

                let values = entries
                    .iter()
                    .map(|(k, v)| v.to_series().rename(&k.name))
                    .collect();

                StructArray::new(struct_field, values, None).into_series()
            }
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
            Self::Float64(val) => write!(buffer, "{}", val),
            Self::Utf8(val) => write!(buffer, "'{}'", val),
            Self::Date(val) => write!(buffer, "DATE '{}'", display_date32(*val)),
            // The `display_timestamp` function formats a timestamp in the ISO 8601 format: "YYYY-MM-DDTHH:MM:SS.fffff".
            // ANSI SQL standard uses a space instead of 'T'. Some databases do not support 'T', hence it's replaced with a space.
            // Reference: https://docs.actian.com/ingres/10s/index.html#page/SQLRef/Summary_of_ANSI_Date_2fTime_Data_Types.html
            Self::Timestamp(val, tu, tz) => write!(
                buffer,
                "TIMESTAMP '{}'",
                display_timestamp(*val, tu, tz).replace('T', " ")
            ),
            // TODO(Colin): Implement the rest of the types in future work for SQL pushdowns.
            Self::Decimal(..)
            | Self::Series(..)
            | Self::Time(..)
            | Self::Binary(..)
            | Self::FixedSizeBinary(..)
            | Self::Duration(..)
            | Self::Interval(..) => display_sql_err,
            #[cfg(feature = "python")]
            Self::Python(..) => display_sql_err,
            Self::Struct(..) => display_sql_err,
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
            Self::UInt32(i) => return Ok(Some(*i as usize)),
            Self::UInt64(i) => return Ok(Some(*i as usize)),
            _ => return Ok(None),
        }
        .map_err(|e| DaftError::ValueError(format!("Failed to convert literal to usize: {}", e)))
    }

    /// If the literal is `Float64`, return it. Otherwise, return None.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float64(f) => Some(*f),
            _ => None,
        }
    }

    /// If the literal is a series, return it. Otherwise, return None.
    pub fn as_series(&self) -> Option<&Series> {
        match self {
            Self::Series(series) => Some(series),
            _ => None,
        }
    }

    /// If the literal is a struct, return the reference to its map. Otherwise, return None.
    pub fn as_struct(&self) -> Option<&IndexMap<Field, Self>> {
        match self {
            Self::Struct(map) => Some(map),
            _ => None,
        }
    }

    pub fn try_from_single_value_series(s: &Series) -> DaftResult<Self> {
        ensure!(s.len() == 1, ValueError: "expected a scalar value");
        Self::get_from_series(s, 0)
    }

    /// instead of type checking a series such as
    /// `series.utf8().get(idx)`
    /// this allows you to get any value as a `LiteralValue` from a series
    /// LiteralValue::get_from_series(s, 0) -> LiteralValue
    pub fn get_from_series(s: &Series, idx: usize) -> DaftResult<Self> {
        let err = || {
            DaftError::ValueError(format!(
                "index {} out of bounds for series of length {}",
                idx,
                s.len()
            ))
        };
        match s.data_type() {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => Ok(s.bool()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::Int8 => Ok(s.i8()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::UInt8 => Ok(s.u8()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::Int16 => Ok(s.i16()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::UInt16 => Ok(s.u16()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::Int32 => Ok(s.i32()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::UInt32 => Ok(s.u32()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::Int64 => Ok(s.i64()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::UInt64 => Ok(s.u64()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::Float64 => Ok(s.f64()?.get(idx).ok_or_else(err)?.literal_value()),
            DataType::Float32 => todo!(),
            DataType::Decimal128(precision, scale) => Ok(Self::Decimal(
                s.decimal128()?.get(idx).ok_or_else(err)?,
                *precision as _,
                *scale as _,
            )),
            DataType::Timestamp(tu, tz) => Ok(Self::Timestamp(
                s.timestamp()?.get(idx).ok_or_else(err)?,
                *tu,
                tz.clone(),
            )),
            DataType::Date => Ok(Self::Date(s.date()?.get(idx).ok_or_else(err)?)),
            DataType::Time(time_unit) => {
                Ok(Self::Time(s.time()?.get(idx).ok_or_else(err)?, *time_unit))
            }
            DataType::Duration(time_unit) => Ok(Self::Duration(
                s.duration()?.get(idx).ok_or_else(err)?,
                *time_unit,
            )),
            DataType::Interval => Ok(Self::Interval(
                s.interval()?.get(idx).ok_or_else(err)?.into(),
            )),
            DataType::Binary => Ok(Self::Binary(s.binary()?.get(idx).ok_or_else(err)?.into())),
            DataType::FixedSizeBinary(n) => Ok(Self::FixedSizeBinary(
                s.fixed_size_binary()?.get(idx).ok_or_else(err)?.into(),
                *n,
            )),
            DataType::Utf8 => Ok(Self::Utf8(s.utf8()?.get(idx).ok_or_else(err)?.into())),
            DataType::FixedSizeList(_, _) => {
                let lst = s.fixed_size_list()?.get(idx).ok_or_else(err)?;
                Ok(lst.literal_value())
            }
            DataType::List(_) => {
                let lst = s.list()?.get(idx).ok_or_else(err)?;
                Ok(lst.literal_value())
            }
            DataType::Struct(fields) => {
                let children = &s.struct_()?.children;
                let mut map = IndexMap::new();
                for (field, child) in fields.iter().zip(children) {
                    map.insert(field.clone(), Self::try_from_single_value_series(child)?);
                }
                Ok(Self::Struct(map))
            }

            #[cfg(feature = "python")]
            DataType::Python => {
                let v = s.python()?.get(idx);

                Ok(Self::Python(PyObjectWrapper(v)))
            }
            ty => Err(DaftError::ValueError(format!(
                "Unsupported data type: {}",
                ty
            ))),
        }
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
        let iter_with_types = iter
            .into_iter()
            .map(|(name, lit)| (Field::new(name, lit.get_type()), lit));
        Self::Struct(IndexMap::from_iter(iter_with_types))
    }
}

pub trait Literal: Sized {
    /// [Literal](Expr::Literal) expression.
    fn lit(self) -> ExprRef {
        Expr::Literal(self.literal_value()).into()
    }
    fn literal_value(self) -> LiteralValue;
}

pub trait FromLiteral: Sized {
    fn try_from_literal(lit: &LiteralValue) -> DaftResult<Self>;
}

pub fn lit<L: Literal>(t: L) -> ExprRef {
    t.lit()
}

pub fn literal_value<L: Literal>(t: L) -> LiteralValue {
    t.literal_value()
}

pub fn null_lit() -> ExprRef {
    Arc::new(Expr::Literal(LiteralValue::Null))
}

/// Convert a slice of literals to a series.
/// This function will return an error if the literals are not all the same type
pub fn literals_to_series(values: &[LiteralValue]) -> DaftResult<Series> {
    use daft_core::{datatypes::*, series::IntoSeries};

    let first_non_null = values.iter().find(|v| !matches!(v, LiteralValue::Null));
    let Some(first_non_null) = first_non_null else {
        return Ok(Series::full_null("literal", &DataType::Null, values.len()));
    };

    let dtype = first_non_null.get_type();

    // make sure all dtypes are the same, or null
    if !values.windows(2).all(|w| {
        w[0].get_type() == w[1].get_type()
            || matches!(w, [LiteralValue::Null, _] | [_, LiteralValue::Null])
    }) {
        return Err(DaftError::ValueError(format!(
            "All literals must have the same data type. Found: {:?}",
            values.iter().map(|lit| lit.get_type()).collect::<Vec<_>>()
        )));
    }

    macro_rules! unwrap_unchecked {
        ($expr:expr, $variant:ident) => {
            match $expr {
                LiteralValue::$variant(val, ..) => Some(val.clone()),
                LiteralValue::Null => None,
                _ => unreachable!("datatype is already checked"),
            }
        };
    }
    macro_rules! unwrap_unchecked_ref {
        ($expr:expr, $variant:ident) => {
            match $expr {
                LiteralValue::$variant(val) => Some(val.clone()),
                LiteralValue::Null => None,
                _ => unreachable!("datatype is already checked"),
            }
        };
    }

    Ok(match dtype {
        DataType::Null => NullArray::full_null("literal", &dtype, values.len()).into_series(),
        DataType::Boolean => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Boolean));
            BooleanArray::from_iter("literal", data).into_series()
        }
        DataType::Utf8 => {
            let data = values.iter().map(|lit| unwrap_unchecked_ref!(lit, Utf8));
            Utf8Array::from_iter("literal", data).into_series()
        }
        DataType::Binary => {
            let data = values.iter().map(|lit| unwrap_unchecked_ref!(lit, Binary));
            BinaryArray::from_iter("literal", data).into_series()
        }
        DataType::Int8 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Int8));
            Int8Array::from_iter(Field::new("literal", DataType::Int8), data).into_series()
        }
        DataType::UInt8 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, UInt8));
            UInt8Array::from_iter(Field::new("literal", DataType::UInt8), data).into_series()
        }
        DataType::Int16 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Int16));
            Int16Array::from_iter(Field::new("literal", DataType::Int16), data).into_series()
        }
        DataType::UInt16 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, UInt16));
            UInt16Array::from_iter(Field::new("literal", DataType::UInt16), data).into_series()
        }
        DataType::Int32 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Int32));
            Int32Array::from_iter(Field::new("literal", DataType::Int32), data).into_series()
        }
        DataType::UInt32 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, UInt32));
            UInt32Array::from_iter(Field::new("literal", DataType::UInt32), data).into_series()
        }
        DataType::Int64 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Int64));
            Int64Array::from_iter(Field::new("literal", DataType::Int64), data).into_series()
        }
        DataType::UInt64 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, UInt64));
            UInt64Array::from_iter(Field::new("literal", DataType::UInt64), data).into_series()
        }
        DataType::Interval => {
            let data = values.iter().map(|lit| match lit {
                LiteralValue::Interval(iv) => Some((iv.months, iv.days, iv.nanoseconds)),
                LiteralValue::Null => None,
                _ => unreachable!("datatype is already checked"),
            });
            IntervalArray::from_iter("literal", data).into_series()
        }
        dtype @ DataType::Timestamp(_, _) => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Timestamp));
            let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);
            TimestampArray::new(Field::new("literal", dtype), physical).into_series()
        }
        dtype @ DataType::Date => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Date));
            let physical = Int32Array::from_iter(Field::new("literal", DataType::Int32), data);
            DateArray::new(Field::new("literal", dtype), physical).into_series()
        }
        dtype @ DataType::Time(_) => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Time));
            let physical = Int64Array::from_iter(Field::new("literal", DataType::Int64), data);

            TimeArray::new(Field::new("literal", dtype), physical).into_series()
        }
        DataType::Float64 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Float64));

            Float64Array::from_iter(Field::new("literal", dtype), data).into_series()
        }
        dtype @ DataType::Decimal128 { .. } => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Decimal));

            Decimal128Array::from_iter(Field::new("literal", dtype), data).into_series()
        }
        struct_dtype @ DataType::Struct(_) => {
            let data = values.iter().map(|lit| lit.to_series()).collect::<Vec<_>>();

            let sa = StructArray::new(Field::new("literal", struct_dtype), data, None);
            sa.into_series()
        }
        #[cfg(feature = "python")]
        DataType::Python => {
            let pynone = Arc::new(pyo3::Python::with_gil(|py| py.None()));

            let data = values.iter().map(|lit| {
                unwrap_unchecked!(lit, Python).map_or_else(|| pynone.clone(), |pyobj| pyobj.0)
            });

            PythonArray::from(("literal", data.collect::<Vec<_>>())).into_series()
        }
        _ => {
            return Err(DaftError::ValueError(format!(
                "Unsupported data type: {:?}",
                dtype
            )))
        }
    })
}

impl From<LiteralValue> for ExprRef {
    fn from(lit: LiteralValue) -> Self {
        Self::new(Expr::Literal(lit.into()))
    }
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::prelude::*;

    use super::{Literal, LiteralValue};
    use crate::{lit::FromLiteral, literal_value};

    #[test]
    fn test_literals_to_series() {
        let values = vec![
            LiteralValue::UInt64(1),
            LiteralValue::UInt64(2),
            LiteralValue::UInt64(3),
        ];
        let expected = vec![1, 2, 3];
        let expected = UInt64Array::from_values("literal", expected.into_iter());
        let expected = expected.into_series();
        let actual = super::literals_to_series(&values).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_uint16_literals_to_series() {
        let values = vec![
            LiteralValue::UInt16(1),
            LiteralValue::UInt16(2),
            LiteralValue::UInt16(3),
        ];
        let expected = vec![1, 2, 3];
        let expected = UInt16Array::from_values("literal", expected.into_iter());
        let expected = expected.into_series();
        let actual = super::literals_to_series(&values).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_int8_literals_to_series() {
        let values = vec![
            LiteralValue::Int8(1),
            LiteralValue::Int8(2),
            LiteralValue::Int8(3),
        ];
        let expected = vec![1, 2, 3];
        let expected = Int8Array::from_values("literal", expected.into_iter());
        let expected = expected.into_series();
        let actual = super::literals_to_series(&values).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_literals_to_series_null() {
        let values = vec![
            LiteralValue::Null,
            LiteralValue::UInt64(2),
            LiteralValue::UInt64(3),
        ];
        let expected = vec![None, Some(2), Some(3)];
        let expected = UInt64Array::from_iter(
            Field::new("literal", DataType::UInt64),
            expected.into_iter(),
        );
        let expected = expected.into_series();
        let actual = super::literals_to_series(&values).unwrap();
        // Series.eq returns false for nulls
        for (expected, actual) in expected.u64().iter().zip(actual.u64().iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_literals_to_series_mismatched() {
        let values = vec![
            LiteralValue::UInt64(1),
            LiteralValue::Utf8("test".to_string()),
        ];
        let actual = super::literals_to_series(&values);
        assert!(actual.is_err());
    }

    fn roundtrip<V: Literal + FromLiteral + Clone + std::fmt::Debug + PartialEq>(
        value: V,
    ) -> DaftResult<()> {
        let lit = literal_value(value.clone());
        let deserialized = V::try_from_literal(&lit).expect("Failed to deserialize LiteralValue");
        assert_eq!(value, deserialized);
        Ok(())
    }

    #[test]
    fn test_roundtrip() -> DaftResult<()> {
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
