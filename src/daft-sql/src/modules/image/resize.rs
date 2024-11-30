use daft_dsl::{Expr, ExprRef, LiteralValue};
use daft_functions::image::resize::{resize, ImageResize};

use crate::{
    ensure,
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments},
    unsupported_sql_err,
};

pub struct SQLImageResize;

impl TryFrom<SQLFunctionArguments> for ImageResize {
    type Error = crate::error::PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let width = args
            .get_named("w")
            .or_else(|| args.get_positional(0))
            .map(|arg| match arg.as_ref() {
                Expr::Literal(LiteralValue::Int64(i)) => Ok(*i),
                _ => unsupported_sql_err!("Expected width to be a number"),
            })
            .transpose()?
            .ok_or_else(|| {
                PlannerError::unsupported_sql("Expected width to be provided".to_string())
            })?;

        let height = args
            .get_named("h")
            .or_else(|| args.get_positional(1))
            .map(|arg| match arg.as_ref() {
                Expr::Literal(LiteralValue::Int64(i)) => Ok(*i),
                _ => unsupported_sql_err!("Expected height to be a number"),
            })
            .transpose()?
            .ok_or_else(|| {
                PlannerError::unsupported_sql("Expected height to be provided".to_string())
            })?;

        ensure!(width > 0, "Width can not be negative: {width}");
        ensure!(height > 0, "Height can not be negative: {height}");

        Ok(Self {
            width: width as u32,
            height: height as u32,
        })
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
                let ImageResize { width, height } =
                    planner.plan_function_args(args, &["w", "h"], 2)?;
                Ok(resize(input, width, height))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_resize: '{inputs:?}'"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Resizes an image to the specified width and height.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input_image", "width", "height"]
    }
}
