use daft_core::prelude::SchemaRef;
use serde::{Deserialize, Serialize};

/// Options for converting blob data into Daft `RecordBatch`es.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobConvertOptions {
    pub schema: Option<SchemaRef>,
    pub limit: Option<usize>,
    pub required_columns: Option<Vec<String>>,
    pub size: Option<i64>,
    pub last_modified: Option<jiff::Timestamp>,
}

/// Options for reading blob files.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobReadOptions {
    pub buffer_size: Option<usize>,
}

impl BlobReadOptions {
    #[must_use]
    pub fn new(buffer_size: Option<usize>) -> Self {
        Self { buffer_size }
    }
}
