use crate::datatypes::DataType;

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
        use arrow2::datatypes::DataType as ArrowType;
        match self {
            Null => DataType::ArrowType(ArrowType::Null),
            Boolean => DataType::ArrowType(ArrowType::Boolean),
            Utf8 => DataType::ArrowType(ArrowType::LargeUtf8),
            Binary => DataType::ArrowType(ArrowType::LargeBinary),
            Int64 => DataType::ArrowType(ArrowType::Int64),
            Float64 => DataType::ArrowType(ArrowType::Float64),
            _ => panic!("Undefined Type"),
        }
    }
}
