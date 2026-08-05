mod get;
mod map_keys;

use get::GetEvaluator;
use map_keys::MapKeysEvaluator;
use serde::{Deserialize, Serialize};

use super::FunctionEvaluator;
use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MapExpr {
    Get,
    MapKeys,
}

impl MapExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            Self::Get => &GetEvaluator {},
            Self::MapKeys => &MapKeysEvaluator {},
        }
    }
}

pub fn get(input: ExprRef, key: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Map(MapExpr::Get),
        inputs: vec![input, key],
    }
    .into()
}

pub fn map_keys(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Map(MapExpr::MapKeys),
        inputs: vec![input],
    }
    .into()
}
