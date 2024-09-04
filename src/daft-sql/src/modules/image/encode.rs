use common_error::DaftError;
use daft_dsl::{Expr, ExprRef, LiteralValue};

use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments},
    unsupported_sql_err,
};
use daft_functions::image::encode::{encode, ImageEncode};

pub struct SQLImageEncode;

impl TryFrom<SQLFunctionArguments> for ImageEncode {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let image_format = args
            .get_named("image_format")
            .map(|arg| match arg.as_ref() {
                Expr::Literal(LiteralValue::Utf8(s)) => {
                    s.parse().map_err(|e: DaftError| PlannerError::from(e))
                }
                _ => unsupported_sql_err!("Expected image_format to be a string"),
            })
            .transpose()?
            .ok_or_else(|| {
                PlannerError::unsupported_sql("Expected image_format argument".to_string())
            })?;

        Ok(Self { image_format })
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
                let args = planner.plan_function_args(args, &["image_format"], 0)?;
                Ok(encode(input, args))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_encode: '{inputs:?}'"),
        }
    }
}
