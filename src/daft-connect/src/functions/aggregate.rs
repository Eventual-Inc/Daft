use daft_core::count_mode::CountMode;
use daft_dsl::unresolved_col;
use daft_schema::dtype::DataType;
use spark_connect::Expression;

use super::{FunctionModule, SparkFunction, UnaryFunction};
use crate::{
    error::ConnectResult, invalid_argument_err, spark_analyzer::expr_analyzer::analyze_expr,
};

pub struct AggregateFunctions;

impl FunctionModule for AggregateFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_fn("count", CountFunction);
        parent.add_fn("mean", UnaryFunction(|arg| arg.mean()));
        parent.add_fn("stddev", UnaryFunction(|arg| arg.stddev()));
        parent.add_fn("min", UnaryFunction(|arg| arg.min()));
        parent.add_fn("max", UnaryFunction(|arg| arg.max()));
        parent.add_fn("sum", UnaryFunction(|arg| arg.sum()));
    }
}

struct CountFunction;

impl SparkFunction for CountFunction {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        match args {
            [arg] => {
                let arg = analyze_expr(arg)?;

                let arg = if arg.as_literal().and_then(|lit| lit.as_i32()) == Some(1i32) {
                    unresolved_col("*")
                } else {
                    arg
                };

                let count = arg.count(CountMode::All).cast(&DataType::Int64);

                Ok(count)
            }
            _ => invalid_argument_err!("requires exactly one argument"),
        }
    }
}
