use std::sync::Arc;

use crate::datatypes::{DaftDataType, Field};

use super::DataArray;

struct LogicalArray<T: DaftDataType> {
    inner: DataArray<T>,
    logical_field: Arc<Field>,
}
