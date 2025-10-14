use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum LanceKVExpr {
    Get {
        uri: String,
        columns: Option<Vec<String>>,
        on_error: String,
        io_config: Option<IOConfig>,
    },
    BatchGet {
        uri: String,
        columns: Option<Vec<String>>,
        batch_size: usize,
        on_error: String,
        io_config: Option<IOConfig>,
    },
    Exists {
        uri: String,
        on_error: String,
        io_config: Option<IOConfig>,
    },
}

impl LanceKVExpr {
    pub fn get(
        uri: String,
        columns: Option<Vec<String>>,
        on_error: String,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self::Get {
            uri,
            columns,
            on_error,
            io_config,
        }
    }

    pub fn batch_get(
        uri: String,
        columns: Option<Vec<String>>,
        batch_size: usize,
        on_error: String,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self::BatchGet {
            uri,
            columns,
            batch_size,
            on_error,
            io_config,
        }
    }

    pub fn exists(uri: String, on_error: String, io_config: Option<IOConfig>) -> Self {
        Self::Exists {
            uri,
            on_error,
            io_config,
        }
    }
}
