use crate::datatypes::DataType;
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
            Null => DataType::Null,
            Boolean(_) => DataType::Boolean,
            Utf8(_) => DataType::Utf8,
            Binary(_) => DataType::Binary,
            Int64(_) => DataType::Int64,
            Float64(_) => DataType::Float64,
        }
    }

    // pub fn to_series(&self) -> DaftResult<Series> {
    //     use arrow2::array::*;
    //     use LiteralValue::*;

    //     let arrow_array = match self {
    //         Null => new_null_array(DataType::Null.to_arrow()?, 1),
    //         Boolean(val) => BooleanArray::from_slice([*val]).boxed(),
    //         Utf8(val) => Utf8Array::<i64>::from([val.into()]).boxed(),
    //         Binary(val) => BinaryArray::<i64>::from([val.into()]).boxed(),
    //         Int64(val) => Int64Array::from_slice([*val]).boxed(),
    //         Float64(val) => Float64Array::from_slice([*val]).boxed(),
    //     };

    //     Ok(Series::from(arrow_array))
    // }
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
