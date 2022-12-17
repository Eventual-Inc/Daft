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
