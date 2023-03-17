mod year;

use serde::{Deserialize, Serialize};

use crate::dsl::{functions::temporal::year::YearEvaluator, Expr};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TemporalExpr {
    Year,
}

impl TemporalExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use TemporalExpr::*;
        match self {
            Year => &YearEvaluator {},
        }
    }
}

pub fn year(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Temporal(TemporalExpr::Year),
        inputs: vec![input.clone()],
    }
}
