mod field;

use field::FieldEvaluator;
use serde::{Deserialize, Serialize};

use crate::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StructExpr {
    Field(String),
}

impl StructExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use StructExpr::*;
        match self {
            Field(_) => &FieldEvaluator {},
        }
    }
}

pub fn field(input: &Expr, name: &str) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Struct(StructExpr::Field(name.to_string())),
        inputs: vec![input.clone()],
    }
}
