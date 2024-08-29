use daft_dsl::{Expr, ExprRef, LiteralValue};
use sqlparser::ast::{FunctionArg, FunctionArgOperator};

use crate::{
    error::SQLPlannerResult,
    functions::{FromSQLArgs, SQLFunction},
    unsupported_sql_err,
};
use daft_functions::image::resize::{resize, ImageResize};

pub struct SQLImageResize;

impl FromSQLArgs for ImageResize {
    fn from_sql(
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<Self> {
        let mut width = None;
        let mut height = None;

        for arg in inputs {
            match arg {
                FunctionArg::Named {
                    name,
                    arg,
                    operator: FunctionArgOperator::Assignment,
                } => match name.value.as_ref() {
                    "w" | "width" => {
                        let arg = planner.try_unwrap_function_arg_expr(arg)?;
                        width = Some(match arg.as_ref() {
                            Expr::Literal(LiteralValue::Int64(i)) => *i,
                            _ => unsupported_sql_err!("Expected width to be a number"),
                        });
                    }
                    "h" | "height" => {
                        let arg = planner.try_unwrap_function_arg_expr(arg)?;
                        height = Some(match arg.as_ref() {
                            Expr::Literal(LiteralValue::Int64(i)) => *i,
                            _ => unsupported_sql_err!("Expected height to be a number"),
                        });
                    }
                    name => unsupported_sql_err!("Unexpected argument: '{name}'"),
                },

                other => {
                    unsupported_sql_err!("Invalid arguments for image_decode: '{}'", other)
                }
            }
        }
        match (width, height) {
            (Some(w), Some(h)) => {
                if w < 0 {
                    unsupported_sql_err!("Width can not be negative: {w}")
                }
                if h < 0 {
                    unsupported_sql_err!("Height can not be negative: {h}")
                }

                Ok(Self {
                    width: w as u32,
                    height: h as u32,
                })
            }
            _ => {
                unsupported_sql_err!("Expected both width and height to be provided")
            }
        }
    }
}

impl SQLFunction for SQLImageResize {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let ImageResize { width, height } = ImageResize::from_sql(args, planner)?;
                Ok(resize(input, width, height))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_resize: '{inputs:?}'"),
        }
    }
}
