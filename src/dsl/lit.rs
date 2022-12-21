use crate::datatypes::{ArrowType, DataType};
use crate::dsl::expr::Expr;

/// Stores a literal value for queries and computations.
/// We only need to support the limited types below since those are the types that we would get from python.
#[derive(Debug)]
pub enum LiteralValue {
    Null,
    /// A binary true or false.
    Boolean(bool),
    /// A UTF8 encoded string type.
    Utf8(String),
    /// A raw binary array
    Binary(Vec<u8>),
    /// A 64-bit integer number.
    Int64(i64),
    /// A 64-bit floating point number.
    Float64(f64),
}

impl LiteralValue {
    pub fn get_type(&self) -> DataType {
        use LiteralValue::*;
        match self {
            Null => DataType::Arrow(ArrowType::Null),
            Boolean(_) => DataType::Arrow(ArrowType::Boolean),
            Utf8(_) => DataType::Arrow(ArrowType::LargeUtf8),
            Binary(_) => DataType::Arrow(ArrowType::LargeBinary),
            Int64(_) => DataType::Arrow(ArrowType::Int64),
            Float64(_) => DataType::Arrow(ArrowType::Float64),
        }
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

make_literal!(bool, Boolean);
make_literal!(f64, Float64);
make_literal!(i64, Int64);

pub fn lit<L: Literal>(t: L) -> Expr {
    t.lit()
}
