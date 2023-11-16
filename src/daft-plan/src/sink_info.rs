use common_io_config::IOConfig;
use daft_dsl::Expr;

use crate::FileFormat;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SinkInfo {
    OutputFileInfo(OutputFileInfo),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputFileInfo {
    pub root_dir: String,
    pub file_format: FileFormat,
    pub partition_cols: Option<Vec<Expr>>,
    pub compression: Option<String>,
    pub io_config: Option<IOConfig>,
}

impl OutputFileInfo {
    pub fn new(
        root_dir: String,
        file_format: FileFormat,
        partition_cols: Option<Vec<Expr>>,
        compression: Option<String>,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            root_dir,
            file_format,
            partition_cols,
            compression,
            io_config,
        }
    }
}
