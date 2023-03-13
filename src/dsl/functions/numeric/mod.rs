mod abs;

use abs::AbsEvaluator;
use serde::{Deserialize, Serialize};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NumericExpr {
    Abs,
}

impl NumericExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use NumericExpr::*;
        match self {
            Abs => &AbsEvaluator {},
        }
    }
}
