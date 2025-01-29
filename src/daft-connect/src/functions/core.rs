use daft_core::count_mode::CountMode;
use daft_dsl::{binary_op, col, ExprRef, Operator};
use daft_schema::dtype::DataType;
use spark_connect::Expression;

use super::{FunctionModule, SparkFunction};
use crate::{
    error::{ConnectError, ConnectResult},
    invalid_argument_err,
    spark_analyzer::SparkAnalyzer,
};

// Core functions are the most basic functions such as `+`, `-`, `*`, `/`, not, notnull, etc.
pub struct CoreFunctions;

impl FunctionModule for CoreFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_fn("==", BinaryOpFunction(Operator::Eq));
        parent.add_fn("!=", BinaryOpFunction(Operator::NotEq));
        parent.add_fn("<", BinaryOpFunction(Operator::Lt));
        parent.add_fn("<=", BinaryOpFunction(Operator::LtEq));
        parent.add_fn(">", BinaryOpFunction(Operator::Gt));
        parent.add_fn(">=", BinaryOpFunction(Operator::GtEq));
        parent.add_fn("+", BinaryOpFunction(Operator::Plus));
        parent.add_fn("-", BinaryOpFunction(Operator::Minus));
        parent.add_fn("*", BinaryOpFunction(Operator::Multiply));
        parent.add_fn("/", BinaryOpFunction(Operator::TrueDivide));
        parent.add_fn("//", BinaryOpFunction(Operator::FloorDivide));
        parent.add_fn("%", BinaryOpFunction(Operator::Modulus));
        parent.add_fn("&", BinaryOpFunction(Operator::And));
        parent.add_fn("|", BinaryOpFunction(Operator::Or));
        parent.add_fn("^", BinaryOpFunction(Operator::Xor));
        parent.add_fn("<<", BinaryOpFunction(Operator::ShiftLeft));
        parent.add_fn(">>", BinaryOpFunction(Operator::ShiftRight));
        parent.add_fn("isnotnull", UnaryFunction(|arg| arg.not_null()));
        parent.add_fn("isnull", UnaryFunction(|arg| arg.is_null()));
        parent.add_fn("not", UnaryFunction(|arg| arg.not()));
        parent.add_fn("sum", UnaryFunction(|arg| arg.sum()));
        parent.add_fn("mean", UnaryFunction(|arg| arg.mean()));
        parent.add_fn("stddev", UnaryFunction(|arg| arg.stddev()));
        parent.add_fn("min", UnaryFunction(|arg| arg.min()));
        parent.add_fn("max", UnaryFunction(|arg| arg.max()));
        parent.add_fn("count", CountFunction);
    }
}

pub struct BinaryOpFunction(Operator);
pub struct UnaryFunction(fn(ExprRef) -> ExprRef);
pub struct CountFunction;

impl SparkFunction for BinaryOpFunction {
    fn to_expr(
        &self,
        args: &[Expression],
        analyzer: &SparkAnalyzer,
    ) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(|arg| analyzer.to_daft_expr(arg))
            .collect::<ConnectResult<Vec<_>>>()?;

        let [lhs, rhs] = args.try_into().map_err(|args| {
            ConnectError::invalid_argument(format!(
                "requires exactly two arguments; got {:?}",
                args
            ))
        })?;

        Ok(binary_op(self.0, lhs, rhs))
    }
}

impl SparkFunction for UnaryFunction {
    fn to_expr(
        &self,
        args: &[Expression],
        analyzer: &SparkAnalyzer,
    ) -> ConnectResult<daft_dsl::ExprRef> {
        match args {
            [arg] => {
                let arg = analyzer.to_daft_expr(arg)?;
                Ok(self.0(arg))
            }
            _ => invalid_argument_err!("requires exactly one argument"),
        }
    }
}

impl SparkFunction for CountFunction {
    fn to_expr(
        &self,
        args: &[Expression],
        analyzer: &SparkAnalyzer,
    ) -> ConnectResult<daft_dsl::ExprRef> {
        match args {
            [arg] => {
                let arg = analyzer.to_daft_expr(arg)?;

                let arg = if arg.as_literal().and_then(|lit| lit.as_i32()) == Some(1i32) {
                    col("*")
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
