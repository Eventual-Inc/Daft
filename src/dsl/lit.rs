use crate::datatypes::{ArrowType, DataType};

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
