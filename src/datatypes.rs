use std::sync::Arc;

use arrow2::datatypes::DataType as ArrowDataType;

pub enum DataType {
    ArrowType(ArrowDataType),
    DaftType,
    PythonType(Arc<str>),
    Unknown,
}
