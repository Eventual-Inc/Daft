use std::fmt::{Display, Formatter, Result};

use crate::datatypes::DataType;
use crate::dsl::expr::Expr;
use crate::series::Series;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::dsl::pyobject::DaftPyObject;

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
    /// A 64-bit floating point number.
    Float64(f64),
    /// Python object.
    #[cfg(feature = "python")]
    Python(DaftPyObject),
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
            Float64(val) => write!(f, "{val:.1}"),
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
            Float64(_) => DataType::Float64,
            #[cfg(feature = "python")]
            Python(_) => DataType::Python,
        }
    }

    pub fn to_series(&self) -> Series {
        use crate::array::BaseArray;
        use crate::datatypes::*;
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
            Float64(val) => Float64Array::from(("literal", [*val].as_slice())).into_series(),
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
