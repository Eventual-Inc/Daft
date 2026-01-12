use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

pub mod kv_functions;
pub mod lance;
mod registry;
pub use registry::KVModule;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LanceConfig {
    uri: String,
    io_config: Option<IOConfig>,
    columns: Option<Vec<String>>,
    batch_size: Option<usize>,
    on_error: Option<String>,
    key_column: Option<String>,
}

impl LanceConfig {
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            io_config: None,
            columns: None,
            batch_size: None,
            on_error: None,
            key_column: None,
        }
    }

    pub fn with_io_config(mut self, io_config: IOConfig) -> Self {
        self.io_config = Some(io_config);
        self
    }

    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_on_error(mut self, on_error: String) -> Self {
        self.on_error = Some(on_error);
        self
    }

    pub fn with_key_column(mut self, key_column: String) -> Self {
        self.key_column = Some(key_column);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVConfig {
    lance: Option<LanceConfig>,
}

impl KVConfig {
    pub fn new() -> Self {
        Self { lance: None }
    }

    pub fn with_lance(mut self, config: LanceConfig) -> Self {
        self.lance = Some(config);
        self
    }
}

impl Default for KVConfig {
    fn default() -> Self {
        Self::new()
    }
}

// Re-export session functions
pub use kv_functions::{
    KVBatchGetWithStoreName, KVExistsWithStoreName, KVGetWithStoreName, KVPutWithStoreName,
};
