use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

pub mod kv_functions;
pub mod memory;
mod registry;
pub use registry::KVModule;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LanceConfig {
    uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    io_config: Option<IOConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    on_error: Option<String>,
}

impl LanceConfig {
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            io_config: None,
            columns: None,
            batch_size: None,
            on_error: None,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MemoryConfig {
    name: String,
}

impl MemoryConfig {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVConfig {
    lance: Option<LanceConfig>,
    memory: Option<MemoryConfig>,
}

impl KVConfig {
    pub fn new() -> Self {
        Self {
            lance: None,
            memory: None,
        }
    }

    pub fn with_lance(mut self, config: LanceConfig) -> Self {
        self.lance = Some(config);
        self
    }

    pub fn with_memory(mut self, config: MemoryConfig) -> Self {
        self.memory = Some(config);
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
