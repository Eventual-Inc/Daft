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
                let input = planner.plan_function_arg(&args[0])?;
                let n = planner
                    .plan_function_arg(&args[1])?
                    .as_literal()
                    .and_then(|l| l.as_i64())
                    .ok_or_else(|| {
                        crate::error::PlannerError::unsupported_sql(
                            "Expected integer literal".to_string(),
                        )
                    })
                    .and_then(|n| {
                        if n > i32::MAX as i64 {
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
                let input = planner.plan_function_arg(&args[0])?;
                let w = planner
                    .plan_function_arg(&args[1])?
                    .as_literal()
                    .and_then(|l| l.as_i64())
                    .ok_or_else(|| {
                        crate::error::PlannerError::unsupported_sql(
                            "Expected integer literal".to_string(),
                        )
                    })?;

                Ok(partitioning::iceberg_truncate(input, w))
            }
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
    let args = planner.plan_function_arg(&args[0])?;
    Ok(f(args))
}
