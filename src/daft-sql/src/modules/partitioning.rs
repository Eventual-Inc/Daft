use daft_dsl::functions::partitioning::{self, PartitioningExpr};

use super::SQLModule;
use crate::{
    ensure,
    functions::{SQLFunction, SQLFunctions},
};

pub struct SQLModulePartitioning;

impl SQLModule for SQLModulePartitioning {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("partitioning_years", PartitioningExpr::Years);
        parent.add_fn("partitioning_months", PartitioningExpr::Months);
        parent.add_fn("partitioning_days", PartitioningExpr::Days);
        parent.add_fn("partitioning_hours", PartitioningExpr::Hours);
        parent.add_fn(
            "partitioning_iceberg_bucket",
            PartitioningExpr::IcebergBucket(0),
        );
        parent.add_fn(
            "partitioning_iceberg_truncate",
            PartitioningExpr::IcebergTruncate(0),
        );
    }
}

impl SQLFunction for PartitioningExpr {
    fn to_expr(
        &self,
        args: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match self {
            Self::Years => partitioning_helper(args, planner, "years", partitioning::years),
            Self::Months => partitioning_helper(args, planner, "months", partitioning::months),
            Self::Days => partitioning_helper(args, planner, "days", partitioning::days),
            Self::Hours => partitioning_helper(args, planner, "hours", partitioning::hours),
            Self::IcebergBucket(_) => {
                ensure!(args.len() == 2, "iceberg_bucket takes exactly 2 arguments");
                let input = planner.plan_function_arg(&args[0])?.into_inner();
                let n = planner
                    .plan_function_arg(&args[1])
                    .map(|arg| arg.into_inner())?
                    .as_literal()
                    .and_then(daft_dsl::LiteralValue::as_i64)
                    .ok_or_else(|| {
                        crate::error::PlannerError::unsupported_sql(
                            "Expected integer literal".to_string(),
                        )
                    })
                    .and_then(|n| {
                        if n > i64::from(i32::MAX) {
                            Err(crate::error::PlannerError::unsupported_sql(
                                "Integer literal too large".to_string(),
                            ))
                        } else {
                            Ok(n as i32)
                        }
                    })?;

                Ok(partitioning::iceberg_bucket(input, n))
            }
            Self::IcebergTruncate(_) => {
                ensure!(
                    args.len() == 2,
                    "iceberg_truncate takes exactly 2 arguments"
                );
                let input = planner.plan_function_arg(&args[0])?.into_inner();
                let w = planner
                    .plan_function_arg(&args[1])
                    .map(|arg| arg.into_inner())?
                    .as_literal()
                    .and_then(daft_dsl::LiteralValue::as_i64)
                    .ok_or_else(|| {
                        crate::error::PlannerError::unsupported_sql(
                            "Expected integer literal".to_string(),
                        )
                    })?;

                Ok(partitioning::iceberg_truncate(input, w))
            }
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        match self {
            Self::Years => "Extracts the number of years since epoch time from a datetime expression.".to_string(),
            Self::Months => "Extracts the number of months since epoch time from a datetime expression.".to_string(),
            Self::Days => "Extracts the number of days since epoch time from a datetime expression.".to_string(),
            Self::Hours => "Extracts the number of hours since epoch time from a datetime expression.".to_string(),
            Self::IcebergBucket(_) => "Computes a bucket number for the input expression based the specified number of buckets using an Iceberg-specific hash.".to_string(),
            Self::IcebergTruncate(_) => "Truncates the input expression to a specified width.".to_string(),
        }
    }

    fn arg_names(&self) -> &'static [&'static str] {
        match self {
            Self::Years => &["input"],
            Self::Months => &["input"],
            Self::Days => &["input"],
            Self::Hours => &["input"],
            Self::IcebergBucket(_) => &["input", "num_buckets"],
            Self::IcebergTruncate(_) => &["input", "width"],
        }
    }
}

fn partitioning_helper<F: FnOnce(daft_dsl::ExprRef) -> daft_dsl::ExprRef>(
    args: &[sqlparser::ast::FunctionArg],
    planner: &crate::planner::SQLPlanner,
    method_name: &str,
    f: F,
) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
    ensure!(args.len() == 1, "{} takes exactly 1 argument", method_name);
    let args = planner.plan_function_arg(&args[0])?.into_inner();
    Ok(f(args))
}
