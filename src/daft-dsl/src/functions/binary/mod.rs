use std::sync::Arc;

use daft_io::IOConfig;
use serde::{Deserialize, Serialize};
use upload_to_folder::UploadToFolderEvaluator;

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

mod upload_to_folder;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum BinaryExpr {
    UploadToFolder {
        folder_location: String,
        io_config: Arc<IOConfig>,
    },
}

impl BinaryExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            BinaryExpr::UploadToFolder { .. } => &UploadToFolderEvaluator {},
        }
    }
}

pub fn upload_to_folder(
    data: ExprRef,
    folder_location: &str,
    io_config: Option<IOConfig>,
) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Binary(BinaryExpr::UploadToFolder {
            folder_location: folder_location.to_string(),
            io_config: io_config.unwrap_or_default().into(),
        }),
        inputs: vec![data],
    }
    .into()
}
