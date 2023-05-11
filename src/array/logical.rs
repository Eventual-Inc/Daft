use std::sync::Arc;

use crate::datatypes::{DaftDataType, DataType, Field};

use super::DataArray;

struct LogicalArray<T: DaftDataType> {
    inner: DataArray<T>,
    logical_field: Arc<Field>,
}
