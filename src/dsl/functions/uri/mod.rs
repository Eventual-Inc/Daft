mod download;

use download::DownloadEvaluator;
use serde::{Deserialize, Serialize};

use crate::dsl::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UriExpr {
    Download {
        max_connections: usize,
        raise_error_on_failure: bool,
        multi_thread: bool,
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
    input: &Expr,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Uri(UriExpr::Download {
            max_connections,
            raise_error_on_failure,
            multi_thread,
        }),
        inputs: vec![input.clone()],
    }
}
