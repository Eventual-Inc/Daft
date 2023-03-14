pub mod numeric;
pub mod utf8;

use numeric::NumericExpr;
use serde::{Deserialize, Serialize};
use utf8::Utf8Expr;

use crate::{datatypes::Field, error::DaftResult, schema::Schema, series::Series};

use super::Expr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionExpr {
    Numeric(NumericExpr),
    Utf8(Utf8Expr),
}

pub trait FunctionEvaluator {
    fn fn_name(&self) -> &'static str;
    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field>;
    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series>;
}

impl FunctionExpr {
    #[inline]
    fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use FunctionExpr::*;
        match self {
            Numeric(expr) => expr.get_evaluator(),
            Utf8(expr) => expr.get_evaluator(),
        }
    }
}

impl FunctionEvaluator for FunctionExpr {
    fn fn_name(&self) -> &'static str {
        self.get_evaluator().fn_name()
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        self.get_evaluator().to_field(inputs, schema)
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        self.get_evaluator().evaluate(inputs)
    }
}
