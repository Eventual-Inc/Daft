use crate::expr::Expr;
use crate::ExprRef;

use common_error::{DaftError, DaftResult};
use common_hashable_float_wrapper::FloatWrapper;
use daft_core::{
    prelude::*,
    utils::display_table::{
        display_date32, display_decimal128, display_series_literal, display_time64,
        display_timestamp,
    },
};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::sync::Arc;
use std::{
    fmt::{Display, Formatter, Result},
    hash::{Hash, Hasher},
};

#[cfg(feature = "python")]
use crate::pyobj_serde::PyObjectWrapper;

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
    /// A 64-bit floating point number.
    Float64(f64),
    /// An [`i128`] representing a decimal number with the provided precision and scale.
    Decimal(i128, u8, i8),
    /// A list
    Series(Series),
    /// Python object.
    #[cfg(feature = "python")]
    Python(PyObjectWrapper),
}

impl Eq for LiteralValue {}

impl Hash for LiteralValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use LiteralValue::*;

        match self {
            // Stable hash for Null variant.
            Null => 1.hash(state),
            Boolean(bool) => bool.hash(state),
            Utf8(s) => s.hash(state),
            Binary(arr) => arr.hash(state),
            Int32(n) => n.hash(state),
            UInt32(n) => n.hash(state),
            Int64(n) => n.hash(state),
            UInt64(n) => n.hash(state),
            Date(n) => n.hash(state),
            Time(n, tu) => {
                n.hash(state);
                tu.hash(state);
            }
            Timestamp(n, tu, tz) => {
                n.hash(state);
                tu.hash(state);
                tz.hash(state);
            }
            // Wrap float64 in hashable newtype.
            Float64(n) => FloatWrapper(*n).hash(state),
            Decimal(n, precision, scale) => {
                n.hash(state);
                precision.hash(state);
                scale.hash(state);
            }
            Series(series) => {
                let hash_result = series.hash(None);
                match hash_result {
                    Ok(hash) => hash.into_iter().for_each(|i| i.hash(state)),
                    Err(_) => panic!("Cannot hash series"),
                }
            }
            #[cfg(feature = "python")]
            Python(py_obj) => py_obj.hash(state),
        }
    }
}

impl Display for LiteralValue {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        use LiteralValue::*;
        match self {
            Null => write!(f, "Null"),
            Boolean(val) => write!(f, "{val}"),
            Utf8(val) => write!(f, "\"{val}\""),
            Binary(val) => write!(f, "Binary[{}]", val.len()),
            Int32(val) => write!(f, "{val}"),
            UInt32(val) => write!(f, "{val}"),
            Int64(val) => write!(f, "{val}"),
            UInt64(val) => write!(f, "{val}"),
            Date(val) => write!(f, "{}", display_date32(*val)),
            Time(val, tu) => write!(f, "{}", display_time64(*val, tu)),
            Timestamp(val, tu, tz) => write!(f, "{}", display_timestamp(*val, tu, tz)),
            Float64(val) => write!(f, "{val:.1}"),
            Decimal(val, precision, scale) => {
                write!(f, "{}", display_decimal128(*val, *precision, *scale))
            }
            Series(series) => write!(f, "{}", display_series_literal(series)),
            #[cfg(feature = "python")]
            Python(pyobj) => write!(f, "PyObject({})", {
                use pyo3::prelude::*;
                Python::with_gil(|py| pyobj.0.call_method0(py, pyo3::intern!(py, "__str__")))
                    .unwrap()
            }),
        }
    }
}

impl LiteralValue {
    pub fn get_type(&self) -> DataType {
        use LiteralValue::*;
        match self {
            Null => DataType::Null,
            Boolean(_) => DataType::Boolean,
            Utf8(_) => DataType::Utf8,
            Binary(_) => DataType::Binary,
            Int32(_) => DataType::Int32,
            UInt32(_) => DataType::UInt32,
            Int64(_) => DataType::Int64,
            UInt64(_) => DataType::UInt64,
            Date(_) => DataType::Date,
            Time(_, tu) => DataType::Time(*tu),
            Timestamp(_, tu, tz) => DataType::Timestamp(*tu, tz.clone()),
            Float64(_) => DataType::Float64,
            Decimal(_, precision, scale) => {
                DataType::Decimal128(*precision as usize, *scale as usize)
            }
            Series(series) => series.data_type().clone(),
            #[cfg(feature = "python")]
            Python(_) => DataType::Python,
        }
    }

    pub fn to_series(&self) -> Series {
        use LiteralValue::*;
        let result = match self {
            Null => NullArray::full_null("literal", &DataType::Null, 1).into_series(),
            Boolean(val) => BooleanArray::from(("literal", [*val].as_slice())).into_series(),
            Utf8(val) => Utf8Array::from(("literal", [val.as_str()].as_slice())).into_series(),
            Binary(val) => BinaryArray::from(("literal", val.as_slice())).into_series(),
            Int32(val) => Int32Array::from(("literal", [*val].as_slice())).into_series(),
            UInt32(val) => UInt32Array::from(("literal", [*val].as_slice())).into_series(),
            Int64(val) => Int64Array::from(("literal", [*val].as_slice())).into_series(),
            UInt64(val) => UInt64Array::from(("literal", [*val].as_slice())).into_series(),
            Date(val) => {
                let physical = Int32Array::from(("literal", [*val].as_slice()));
                DateArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Time(val, ..) => {
                let physical = Int64Array::from(("literal", [*val].as_slice()));
                TimeArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Timestamp(val, ..) => {
                let physical = Int64Array::from(("literal", [*val].as_slice()));
                TimestampArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Float64(val) => Float64Array::from(("literal", [*val].as_slice())).into_series(),
            Decimal(val, ..) => {
                let physical = Int128Array::from(("literal", [*val].as_slice()));
                Decimal128Array::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Series(series) => series.clone().rename("literal"),
            #[cfg(feature = "python")]
            Python(val) => PythonArray::from(("literal", vec![val.0.clone()])).into_series(),
        };
        result
    }

    pub fn display_sql<W: Write>(&self, buffer: &mut W) -> io::Result<()> {
        use LiteralValue::*;
        let display_sql_err = Err(io::Error::new(
            io::ErrorKind::Other,
            "Unsupported literal for SQL translation",
        ));
        match self {
            Null => write!(buffer, "NULL"),
            Boolean(v) => write!(buffer, "{}", v),
            Int32(val) => write!(buffer, "{}", val),
            UInt32(val) => write!(buffer, "{}", val),
            Int64(val) => write!(buffer, "{}", val),
            UInt64(val) => write!(buffer, "{}", val),
            Float64(val) => write!(buffer, "{}", val),
            Utf8(val) => write!(buffer, "'{}'", val),
            Date(val) => write!(buffer, "DATE '{}'", display_date32(*val)),
            // The `display_timestamp` function formats a timestamp in the ISO 8601 format: "YYYY-MM-DDTHH:MM:SS.fffff".
            // ANSI SQL standard uses a space instead of 'T'. Some databases do not support 'T', hence it's replaced with a space.
            // Reference: https://docs.actian.com/ingres/10s/index.html#page/SQLRef/Summary_of_ANSI_Date_2fTime_Data_Types.html
            Timestamp(val, tu, tz) => write!(
                buffer,
                "TIMESTAMP '{}'",
                display_timestamp(*val, tu, tz).replace('T', " ")
            ),
            // TODO(Colin): Implement the rest of the types in future work for SQL pushdowns.
            Decimal(..) | Series(..) | Time(..) | Binary(..) => display_sql_err,
            #[cfg(feature = "python")]
            Python(..) => display_sql_err,
        }
    }

    /// If the liter is a boolean, return it. Otherwise, return None.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            LiteralValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// If the literal is a string, return it. Otherwise, return None.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            LiteralValue::Utf8(s) => Some(s),
            _ => None,
        }
    }
    /// If the literal is `Binary`, return it. Otherwise, return None.
    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            LiteralValue::Binary(b) => Some(b),
            _ => None,
        }
    }
    /// If the literal is `Int32`, return it. Otherwise, return None.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            LiteralValue::Int32(i) => Some(*i),
            _ => None,
        }
    }
    /// If the literal is `UInt32`, return it. Otherwise, return None.
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            LiteralValue::UInt32(i) => Some(*i),
            _ => None,
        }
    }
    /// If the literal is `Int64`, return it. Otherwise, return None.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            LiteralValue::Int64(i) => Some(*i),
            _ => None,
        }
    }
    /// If the literal is `UInt64`, return it. Otherwise, return None.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            LiteralValue::UInt64(i) => Some(*i),
            _ => None,
        }
    }
    /// If the literal is `Float64`, return it. Otherwise, return None.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            LiteralValue::Float64(f) => Some(*f),
            _ => None,
        }
    }
    /// If the literal is a series, return it. Otherwise, return None.
    pub fn as_series(&self) -> Option<&Series> {
        match self {
            LiteralValue::Series(series) => Some(series),
            _ => None,
        }
    }
}

pub trait Literal {
    /// [Literal](Expr::Literal) expression.
    fn lit(self) -> ExprRef;
}

impl Literal for String {
    fn lit(self) -> ExprRef {
        Expr::Literal(LiteralValue::Utf8(self)).into()
    }
}

impl<'a> Literal for &'a str {
    fn lit(self) -> ExprRef {
        Expr::Literal(LiteralValue::Utf8(self.to_owned())).into()
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        impl Literal for $TYPE {
            fn lit(self) -> ExprRef {
                Expr::Literal(LiteralValue::$SCALAR(self)).into()
            }
        }
    };
}

impl<'a> Literal for &'a [u8] {
    fn lit(self) -> ExprRef {
        Expr::Literal(LiteralValue::Binary(self.to_vec())).into()
    }
}

impl Literal for Series {
    fn lit(self) -> ExprRef {
        Expr::Literal(LiteralValue::Series(self)).into()
    }
}

#[cfg(feature = "python")]
impl Literal for pyo3::PyObject {
    fn lit(self) -> ExprRef {
        Expr::Literal(LiteralValue::Python(PyObjectWrapper(self))).into()
    }
}

make_literal!(bool, Boolean);
make_literal!(i32, Int32);
make_literal!(u32, UInt32);
make_literal!(i64, Int64);
make_literal!(u64, UInt64);
make_literal!(f64, Float64);

pub fn lit<L: Literal>(t: L) -> ExprRef {
    t.lit()
}

pub fn null_lit() -> ExprRef {
    Arc::new(Expr::Literal(LiteralValue::Null))
}

/// Convert a slice of literals to a series.
/// This function will return an error if the literals are not all the same type
pub fn literals_to_series(values: &[LiteralValue]) -> DaftResult<Series> {
    use daft_core::datatypes::*;
    use daft_core::series::IntoSeries;

    let dtype = values[0].get_type();

    // make sure all dtypes are the same
    if !values
        .windows(2)
        .all(|w| w[0].get_type() == w[1].get_type())
    {
        return Err(DaftError::ValueError(format!(
            "All literals must have the same data type. Found: {:?}",
            values.iter().map(|lit| lit.get_type()).collect::<Vec<_>>()
        )));
    }

    macro_rules! unwrap_unchecked {
        ($expr:expr, $variant:ident) => {
            match $expr {
                LiteralValue::$variant(val, ..) => *val,
                _ => unreachable!("datatype is already checked"),
            }
        };
    }
    macro_rules! unwrap_unchecked_ref {
        ($expr:expr, $variant:ident) => {
            match $expr {
                LiteralValue::$variant(val) => val.clone(),
                _ => unreachable!("datatype is already checked"),
            }
        };
    }

    Ok(match dtype {
        DataType::Null => NullArray::full_null("literal", &dtype, values.len()).into_series(),
        DataType::Boolean => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Boolean));
            BooleanArray::from_values("literal", data).into_series()
        }
        DataType::Utf8 => {
            let data = values.iter().map(|lit| unwrap_unchecked_ref!(lit, Utf8));
            Utf8Array::from_values("literal", data).into_series()
        }
        DataType::Binary => {
            let data = values.iter().map(|lit| unwrap_unchecked_ref!(lit, Binary));
            BinaryArray::from_values("literal", data).into_series()
        }
        DataType::Int32 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Int32));
            Int32Array::from_values("literal", data).into_series()
        }
        DataType::UInt32 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, UInt32));
            UInt32Array::from_values("literal", data).into_series()
        }
        DataType::Int64 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Int64));
            Int64Array::from_values("literal", data).into_series()
        }
        DataType::UInt64 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, UInt64));
            UInt64Array::from_values("literal", data).into_series()
        }

        dtype @ DataType::Timestamp(_, _) => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Timestamp));
            let physical = Int64Array::from_values("literal", data);
            TimestampArray::new(Field::new("literal", dtype), physical).into_series()
        }
        dtype @ DataType::Date => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Date));
            let physical = Int32Array::from_values("literal", data);
            DateArray::new(Field::new("literal", dtype), physical).into_series()
        }
        dtype @ DataType::Time(_) => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Time));
            let physical = Int64Array::from_values("literal", data);
            TimeArray::new(Field::new("literal", dtype), physical).into_series()
        }
        DataType::Float64 => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Float64));

            Float64Array::from_values("literal", data).into_series()
        }
        dtype @ DataType::Decimal128 { .. } => {
            let data = values.iter().map(|lit| unwrap_unchecked!(lit, Decimal));

            let physical = Int128Array::from_values("literal", data);
            Decimal128Array::new(Field::new("literal", dtype), physical).into_series()
        }
        _ => {
            return Err(DaftError::ValueError(format!(
                "Unsupported data type: {:?}",
                dtype
            )))
        }
    })
}

#[cfg(test)]
mod test {
    use daft_core::prelude::*;

    use super::LiteralValue;

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
    fn test_literals_to_series_null() {
        let values = vec![
            LiteralValue::Null,
            LiteralValue::UInt64(2),
            LiteralValue::UInt64(3),
        ];
        let actual = super::literals_to_series(&values);
        assert!(actual.is_err());
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
}
