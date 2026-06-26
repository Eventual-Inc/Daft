use daft_dsl::{Expr, ExprRef, WindowExpr};
use sqlparser::ast::FunctionArg;

use crate::{
    error::SQLPlannerResult, functions::SQLFunction, invalid_operation_err, modules::SQLModule,
    planner::SQLPlanner,
};

pub struct SQLRowNumber;

impl SQLFunction for SQLRowNumber {
    fn to_expr(&self, inputs: &[FunctionArg], _planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(
            inputs.is_empty(),
            "ROW_NUMBER() does not take any arguments"
        );
        Ok(Expr::WindowFunction(WindowExpr::RowNumber).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}() OVER() - Returns the sequential row number starting at 1 within a partition"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLRank;

impl SQLFunction for SQLRank {
    fn to_expr(&self, inputs: &[FunctionArg], _planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(inputs.is_empty(), "RANK() does not take any arguments");
        Ok(Expr::WindowFunction(WindowExpr::Rank).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!("{alias}() OVER() - Returns the rank of the current row within a partition")
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLDenseRank;

impl SQLFunction for SQLDenseRank {
    fn to_expr(&self, inputs: &[FunctionArg], _planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(
            inputs.is_empty(),
            "DENSE_RANK() does not take any arguments"
        );
        Ok(Expr::WindowFunction(WindowExpr::DenseRank).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!("{alias}() OVER() - Returns the dense rank of the current row within a partition")
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLLag;

impl SQLFunction for SQLLag {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(
            !inputs.is_empty() && inputs.len() <= 3,
            "LAG() takes 1, 2 or 3 arguments: LAG(value, offset, default)"
        );

        let value = planner.plan_function_arg(&inputs[0])?.into_inner();

        let offset = if inputs.len() > 1 {
            let offset_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
            if let Some(offset_val) = offset_expr.as_literal().and_then(|lit| lit.as_i64()) {
                offset_val as isize
            } else {
                invalid_operation_err!("offset must be a literal integer")
            }
        } else {
            1
        };

        let default = if inputs.len() > 2 {
            Some(planner.plan_function_arg(&inputs[2])?.into_inner())
        } else {
            None
        };

        let negative_offset = -offset;

        Ok(Expr::WindowFunction(WindowExpr::Offset {
            input: value,
            offset: negative_offset,
            default,
        })
        .arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}(value, offset, default) OVER() - Returns the value of the row offset rows before the current row"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["value", "offset", "default"]
    }
}

pub struct SQLLead;

impl SQLFunction for SQLLead {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(
            !inputs.is_empty() && inputs.len() <= 3,
            "LEAD() takes 1, 2 or 3 arguments: LEAD(value, offset, default)"
        );

        let value = planner.plan_function_arg(&inputs[0])?.into_inner();

        let offset = if inputs.len() > 1 {
            let offset_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
            if let Some(offset_val) = offset_expr.as_literal().and_then(|lit| lit.as_i64()) {
                offset_val as isize
            } else {
                invalid_operation_err!("offset must be a literal integer")
            }
        } else {
            1
        };

        let default = if inputs.len() > 2 {
            Some(planner.plan_function_arg(&inputs[2])?.into_inner())
        } else {
            None
        };

        Ok(Expr::WindowFunction(WindowExpr::Offset {
            input: value,
            offset,
            default,
        })
        .arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}(value, offset, default) OVER() - Returns the value of the row offset rows after the current row"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["value", "offset", "default"]
    }
}

pub struct SQLCumeDist;

impl SQLFunction for SQLCumeDist {
    fn to_expr(&self, inputs: &[FunctionArg], _planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(inputs.is_empty(), "CUME_DIST() does not take any arguments");
        Ok(Expr::WindowFunction(WindowExpr::CumeDist).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}() OVER(... ORDER BY ...) - Returns the cumulative distribution of a value within a partition"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLPercentRank;

impl SQLFunction for SQLPercentRank {
    fn to_expr(&self, inputs: &[FunctionArg], _planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        assert!(
            inputs.is_empty(),
            "PERCENT_RANK() does not take any arguments"
        );
        Ok(Expr::WindowFunction(WindowExpr::PercentRank).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}() OVER(... ORDER BY ...) - Returns the relative rank of a row, (rank - 1) / (total_rows - 1)"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[]
    }
}

pub struct SQLNtile;

impl SQLFunction for SQLNtile {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 1 {
            invalid_operation_err!("NTILE() takes exactly 1 argument: NTILE(n)");
        }
        let n_expr = planner.plan_function_arg(&inputs[0])?.into_inner();
        let n = if let Some(v) = n_expr.as_literal().and_then(|lit| lit.as_i64()) {
            v
        } else {
            invalid_operation_err!("ntile(n): n must be a literal integer")
        };
        if n < 1 {
            invalid_operation_err!("ntile(n): n must be >= 1");
        }
        Ok(Expr::WindowFunction(WindowExpr::Ntile(n)).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}(n) OVER(... ORDER BY ...) - Splits an ordered partition into `n` buckets and returns the bucket number (1-based)"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["n"]
    }
}

pub struct SQLFirstValue;

impl SQLFunction for SQLFirstValue {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        if inputs.is_empty() || inputs.len() > 2 {
            invalid_operation_err!(
                "FIRST_VALUE() takes 1 or 2 arguments: FIRST_VALUE(value [, ignore_nulls])"
            );
        }
        let value = planner.plan_function_arg(&inputs[0])?.into_inner();
        let ignore_nulls = if inputs.len() == 2 {
            let ig = planner.plan_function_arg(&inputs[1])?.into_inner();
            ig.as_literal()
                .and_then(|lit| lit.as_bool())
                .ok_or_else(|| crate::error::PlannerError::InvalidOperation {
                    message:
                        "first_value(value, ignore_nulls): ignore_nulls must be a boolean literal"
                            .to_string(),
                })?
        } else {
            false
        };
        Ok(Expr::WindowFunction(WindowExpr::FirstValue(value, ignore_nulls)).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}(value [, ignore_nulls]) OVER(... ROWS BETWEEN ...) - Returns the first value in the window frame"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["value", "ignore_nulls"]
    }
}

pub struct SQLLastValue;

impl SQLFunction for SQLLastValue {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        if inputs.is_empty() || inputs.len() > 2 {
            invalid_operation_err!(
                "LAST_VALUE() takes 1 or 2 arguments: LAST_VALUE(value [, ignore_nulls])"
            );
        }
        let value = planner.plan_function_arg(&inputs[0])?.into_inner();
        let ignore_nulls = if inputs.len() == 2 {
            let ig = planner.plan_function_arg(&inputs[1])?.into_inner();
            ig.as_literal()
                .and_then(|lit| lit.as_bool())
                .ok_or_else(|| crate::error::PlannerError::InvalidOperation {
                    message:
                        "last_value(value, ignore_nulls): ignore_nulls must be a boolean literal"
                            .to_string(),
                })?
        } else {
            false
        };
        Ok(Expr::WindowFunction(WindowExpr::LastValue(value, ignore_nulls)).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}(value [, ignore_nulls]) OVER(... ROWS BETWEEN ...) - Returns the last value in the window frame"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["value", "ignore_nulls"]
    }
}

pub struct SQLNthValue;

impl SQLFunction for SQLNthValue {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        if inputs.len() < 2 || inputs.len() > 3 {
            invalid_operation_err!(
                "NTH_VALUE() takes 2 or 3 arguments: NTH_VALUE(value, n [, ignore_nulls])"
            );
        }
        let value = planner.plan_function_arg(&inputs[0])?.into_inner();
        let n_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
        let n = n_expr
            .as_literal()
            .and_then(|lit| lit.as_i64())
            .ok_or_else(|| crate::error::PlannerError::InvalidOperation {
                message: "nth_value(value, n): n must be a literal integer".to_string(),
            })?;
        if n < 1 {
            invalid_operation_err!("nth_value(value, n): n must be >= 1");
        }
        let ignore_nulls = if inputs.len() == 3 {
            let ig = planner.plan_function_arg(&inputs[2])?.into_inner();
            ig.as_literal()
                .and_then(|lit| lit.as_bool())
                .ok_or_else(|| crate::error::PlannerError::InvalidOperation {
                    message:
                        "nth_value(value, n, ignore_nulls): ignore_nulls must be a boolean literal"
                            .to_string(),
                })?
        } else {
            false
        };
        Ok(Expr::WindowFunction(WindowExpr::NthValue(value, n, ignore_nulls)).arced())
    }

    fn docstrings(&self, alias: &str) -> String {
        format!(
            "{alias}(value, n [, ignore_nulls]) OVER(... ROWS BETWEEN ...) - Returns the n-th value (1-based) in the window frame"
        )
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["value", "n", "ignore_nulls"]
    }
}

pub struct SQLModuleWindow;

impl SQLModule for SQLModuleWindow {
    fn register(registry: &mut crate::functions::SQLFunctions) {
        registry.add_fn("row_number", SQLRowNumber {});
        registry.add_fn("rank", SQLRank {});
        registry.add_fn("dense_rank", SQLDenseRank {});
        registry.add_fn("lag", SQLLag {});
        registry.add_fn("lead", SQLLead {});
        registry.add_fn("cume_dist", SQLCumeDist {});
        registry.add_fn("percent_rank", SQLPercentRank {});
        registry.add_fn("ntile", SQLNtile {});
        registry.add_fn("first_value", SQLFirstValue {});
        registry.add_fn("last_value", SQLLastValue {});
        registry.add_fn("nth_value", SQLNthValue {});
    }
}
