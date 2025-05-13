use daft_dsl::{Expr, ExprRef, LiteralValue};
use daft_functions::image::to_mode::{image_to_mode, ImageToMode};

use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments},
    unsupported_sql_err,
};

pub struct SQLImageToMode;

impl TryFrom<SQLFunctionArguments> for ImageToMode {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let mode = args
            .get_named("mode")
            .map(|arg| match arg.as_ref() {
                Expr::Literal(LiteralValue::Utf8(s)) => s.parse().map_err(PlannerError::from),
                _ => unsupported_sql_err!("Expected mode to be a string"),
            })
            .transpose()?
            .ok_or_else(|| PlannerError::unsupported_sql("Expected mode argument".to_string()))?;

        Ok(Self { mode })
    }
}

impl SQLFunction for SQLImageToMode {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                let ImageToMode { mode } = planner.plan_function_args(args, &["mode"], 0)?;
                Ok(image_to_mode(input, mode))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_encode: '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Converts an image to the specified mode (e.g. RGB, RGBA, Grayscale).".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input_image", "mode"]
    }
}
