mod download;

use std::sync::Arc;

use download::DownloadEvaluator;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

use common_io_config::IOConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum UriExpr {
    Download {
        max_connections: usize,
        raise_error_on_failure: bool,
        multi_thread: bool,
        config: Arc<IOConfig>,
    },
}

impl UriExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use UriExpr::*;
        match self {
            Download { .. } => &DownloadEvaluator {},
        }
    }
}

pub fn download(
    input: ExprRef,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    config: Option<IOConfig>,
) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Uri(UriExpr::Download {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            config: config.unwrap_or_default().into(),
        }),
        inputs: vec![input],
    }
    .into()
}
