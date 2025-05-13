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
        format!("{alias}(value, offset, default) OVER() - Returns the value of the row offset rows before the current row")
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
        format!("{alias}(value, offset, default) OVER() - Returns the value of the row offset rows after the current row")
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["value", "offset", "default"]
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
    }
}
