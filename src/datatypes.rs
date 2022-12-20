use std::sync::Arc;

pub use arrow2::datatypes::DataType as ArrowType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    Arrow(ArrowType),
    DaftType,
    PythonType(Arc<str>),
    Unknown,
}
