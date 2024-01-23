use crate::expr::Expr;

use daft_core::{array::ops::full::FullNull, datatypes::DataType};
use daft_core::{array::ListArray, utils::hashable_float_wrapper::FloatWrapper};
use daft_core::{
    datatypes::{
        logical::{DateArray, TimestampArray},
        TimeUnit,
    },
    series::Series,
    utils::display_table::{display_date32, display_timestamp},
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter, Result},
    hash::{Hash, Hasher},
};

#[cfg(feature = "python")]
use crate::pyobject::DaftPyObject;

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
    /// When the timezone is not specified, the timestamp is considered to have no timezone
    /// and is represented _as is_
    Timestamp(i64, TimeUnit, Option<String>),
    /// An [`i32`] representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days.
    Date(i32),
    /// A 64-bit floating point number.
    Float64(f64),
    /// A list
    List(Vec<LiteralValue>),
    /// Python object.
    #[cfg(feature = "python")]
    Python(DaftPyObject),
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
            Timestamp(n, tu, tz) => {
                n.hash(state);
                tu.hash(state);
                tz.hash(state);
            }
            // Wrap float64 in hashable newtype.
            Float64(n) => FloatWrapper(*n).hash(state),
            List(l) => l.hash(state),
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
            Timestamp(val, tu, tz) => write!(f, "{}", display_timestamp(*val, tu, tz)),
            Float64(val) => write!(f, "{val:.1}"),
            List(val) => write!(f, "{:?}", val),
            #[cfg(feature = "python")]
            Python(pyobj) => write!(f, "PyObject({})", {
                use pyo3::prelude::*;
                Python::with_gil(|py| {
                    pyobj
                        .pyobject
                        .call_method0(py, pyo3::intern!(py, "__str__"))
                })
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
            Timestamp(_, tu, tz) => DataType::Timestamp(*tu, tz.clone()),
            Float64(_) => DataType::Float64,
            List(l) => {
                if let Some(first) = l.first() {
                    DataType::List(Box::new(first.get_type()))
                } else {
                    DataType::Null
                }
            }
            #[cfg(feature = "python")]
            Python(_) => DataType::Python,
        }
    }

    pub fn to_series(&self) -> Series {
        use daft_core::datatypes::*;
        use daft_core::series::IntoSeries;
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
            Timestamp(val, ..) => {
                let physical = Int64Array::from(("literal", [*val].as_slice()));
                TimestampArray::new(Field::new("literal", self.get_type()), physical).into_series()
            }
            Float64(val) => Float64Array::from(("literal", [*val].as_slice())).into_series(),
            List(vals) => {
                let offsets = unsafe {
                    arrow2::offset::Offsets::<i64>::new_unchecked(vec![0, vals.len() as i64])
                };
                if vals.is_empty() {
                    ListArray::new(
                        Field::new("literal", DataType::List(Box::new(DataType::Null))),
                        NullArray::full_null("literal", &DataType::Null, 0).into_series(),
                        offsets.into(),
                        None,
                    )
                    .into_series()
                } else {
                    match vals.first().unwrap() {
                        LiteralValue::Null => ListArray::new(
                            Field::new("literal", DataType::List(Box::new(DataType::Null))),
                            NullArray::full_null("literal", &DataType::Null, vals.len())
                                .into_series(),
                            offsets.into(),
                            None,
                        )
                        .into_series(),
                        LiteralValue::Boolean(..) => {
                            let vals_extracted: Vec<bool> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Boolean(b) => *b,
                                    _ => panic!("Expected boolean, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Boolean))),
                                BooleanArray::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::Utf8(..) => {
                            let vals_extracted: Vec<&str> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Utf8(s) => s.as_str(),
                                    _ => panic!("Expected string, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Utf8))),
                                Utf8Array::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::Binary(..) => {
                            let vals_extracted: Vec<&[u8]> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Binary(b) => b.as_slice(),
                                    _ => panic!("Expected binary, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Binary))),
                                BinaryArray::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::Int32(..) => {
                            let vals_extracted: Vec<i32> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Int32(i) => *i,
                                    _ => panic!("Expected i32, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Int32))),
                                Int32Array::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::UInt32(..) => {
                            let vals_extracted: Vec<u32> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::UInt32(i) => *i,
                                    _ => panic!("Expected u32, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::UInt32))),
                                UInt32Array::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::Int64(..) => {
                            let vals_extracted: Vec<i64> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Int64(i) => *i,
                                    _ => panic!("Expected i64, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Int64))),
                                Int64Array::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::UInt64(..) => {
                            let vals_extracted: Vec<u64> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::UInt64(i) => *i,
                                    _ => panic!("Expected u64, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::UInt64))),
                                UInt64Array::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::Date(..) => {
                            let vals_extracted: Vec<i32> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Date(i) => *i,
                                    _ => panic!("Expected date, found {:?}", item),
                                })
                                .collect();
                            let physical = Int32Array::from(("literal", vals_extracted.as_slice()));
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Date))),
                                DateArray::new(Field::new("literal", self.get_type()), physical)
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::Timestamp(..) => {
                            let vals_extracted: Vec<i64> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Timestamp(i, ..) => *i,
                                    _ => panic!("Expected timestamp, found {:?}", item),
                                })
                                .collect();
                            let physical = Int64Array::from(("literal", vals_extracted.as_slice()));
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(self.get_type()))),
                                TimestampArray::new(
                                    Field::new("literal", self.get_type()),
                                    physical,
                                )
                                .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::Float64(..) => {
                            let vals_extracted: Vec<f64> = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Float64(i) => *i,
                                    _ => panic!("Expected f64, found {:?}", item),
                                })
                                .collect();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Float64))),
                                Float64Array::from(("literal", vals_extracted.as_slice()))
                                    .into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                        LiteralValue::List(..) => {
                            panic!("Nested lists are not supported")
                        }
                        #[cfg(feature = "python")]
                        LiteralValue::Python(..) => {
                            let vals_extracted = vals
                                .iter()
                                .map(|item| match item {
                                    LiteralValue::Python(i) => i.pyobject.clone(),
                                    _ => panic!("Expected PyObject, found {:?}", item),
                                })
                                .collect::<Vec<_>>();
                            ListArray::new(
                                Field::new("literal", DataType::List(Box::new(DataType::Python))),
                                PythonArray::from(("literal", vals_extracted)).into_series(),
                                offsets.into(),
                                None,
                            )
                            .into_series()
                        }
                    }
                }
            }
            #[cfg(feature = "python")]
            Python(val) => PythonArray::from(("literal", vec![val.pyobject.clone()])).into_series(),
        };
        result
    }
}

pub trait Literal {
    /// [Literal](Expr::Literal) expression.
    fn lit(self) -> Expr;
}

impl Literal for String {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Utf8(self))
    }
}

impl<'a> Literal for &'a str {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Utf8(self.to_owned()))
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        impl Literal for $TYPE {
            fn lit(self) -> Expr {
                Expr::Literal(LiteralValue::$SCALAR(self))
            }
        }
    };
}

impl<'a> Literal for &'a [u8] {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Binary(self.to_vec()))
    }
}

impl Literal for Vec<LiteralValue> {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::List(self))
    }
}

#[cfg(feature = "python")]
impl Literal for pyo3::PyObject {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Python(DaftPyObject { pyobject: self }))
    }
}

make_literal!(bool, Boolean);
make_literal!(i32, Int32);
make_literal!(u32, UInt32);
make_literal!(i64, Int64);
make_literal!(u64, UInt64);
make_literal!(f64, Float64);

pub fn lit<L: Literal>(t: L) -> Expr {
    t.lit()
}

pub fn null_lit() -> Expr {
    Expr::Literal(LiteralValue::Null)
}
