use daft_dsl::{Expr, ExprRef, LiteralValue};
use sqlparser::ast::{FunctionArg, FunctionArgOperator};

use crate::{
    error::SQLPlannerResult,
    functions::{FromSQLArgs, SQLFunction},
    unsupported_sql_err,
};
use daft_functions::image::encode::{encode, ImageEncode};

pub struct SQLImageEncode;

impl FromSQLArgs for ImageEncode {
    fn from_sql(
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<Self> {
        match inputs {
            [FunctionArg::Named {
                name,
                arg,
                operator: FunctionArgOperator::Assignment,
            }] if &name.value == "image_format" => {
                let arg = planner.try_unwrap_function_arg_expr(arg)?;
                let image_format = match arg.as_ref() {
                    Expr::Literal(LiteralValue::Utf8(s)) => s.parse()?,
                    _ => unsupported_sql_err!("Expected image_format to be a string"),
                };
                Ok(Self { image_format })
            }
            _ => unsupported_sql_err!("Invalid arguments for image_encode: '{inputs:?}'"),
        }
    }
}

impl SQLFunction for SQLImageEncode {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args = ImageEncode::from_sql(args, planner)?;
                Ok(encode(input, args))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_encode: '{inputs:?}'"),
        }
    }
}
