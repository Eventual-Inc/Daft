mod evaluators;

use serde::{Deserialize, Serialize};

use crate::{
    functions::partitioning::evaluators::{
        DaysEvaluator, HoursEvaluator, IcebergBucketEvaluator, IcebergTruncateEvaluator,
        MonthsEvaluator, YearsEvaluator,
    },
    Expr, ExprRef,
};

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PartitioningExpr {
    Years,
    Months,
    Days,
    Hours,
    IcebergBucket(i32),
    IcebergTruncate(i64),
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
            IcebergBucket(..) => &IcebergBucketEvaluator {},
            IcebergTruncate(..) => &IcebergTruncateEvaluator {},
        }
    }
}

pub fn days(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Days),
        inputs: vec![input],
    }
    .into()
}

pub fn hours(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Hours),
        inputs: vec![input],
    }
    .into()
}

pub fn months(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Months),
        inputs: vec![input],
    }
    .into()
}

pub fn years(input: ExprRef) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::Years),
        inputs: vec![input],
    }
    .into()
}

pub fn iceberg_bucket(input: ExprRef, n: i32) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::IcebergBucket(n)),
        inputs: vec![input],
    }
    .into()
}

pub fn iceberg_truncate(input: ExprRef, w: i64) -> ExprRef {
    Expr::Function {
        func: super::FunctionExpr::Partitioning(PartitioningExpr::IcebergTruncate(w)),
        inputs: vec![input],
    }
    .into()
}
