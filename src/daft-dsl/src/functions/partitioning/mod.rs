mod evaluators;

use serde::{Deserialize, Serialize};

use crate::{
    functions::partitioning::evaluators::{
        DaysEvaluator, HoursEvaluator, MonthsEvaluator, YearsEvaluator,
    },
    Expr,
};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PartitioningExpr {
    Years,
    Months,
    Days,
    Hours,
}

impl PartitioningExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        use PartitioningExpr::*;
        match self {
            Years => &YearsEvaluator {},
            Months => &MonthsEvaluator {},
            Days => &DaysEvaluator {},
            Hours => &HoursEvaluator {},
        }
    }
}

pub fn days(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Days),
        inputs: vec![input.clone()],
    }
}

pub fn hours(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Hours),
        inputs: vec![input.clone()],
    }
}

pub fn months(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Months),
        inputs: vec![input.clone()],
    }
}

pub fn years(input: &Expr) -> Expr {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Years),
        inputs: vec![input.clone()],
    }
}
