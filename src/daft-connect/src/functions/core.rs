use daft_dsl::{binary_op, Operator};
use daft_functions::{coalesce::Coalesce, float::IsNan};
use daft_sql::sql_expr;
use spark_connect::Expression;

use super::{FunctionModule, SparkFunction, UnaryFunction, TODO_FUNCTION};
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
        // Normal Functions
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#normal-functions

        parent.add_fn("coalesce", Coalesce {});
        parent.add_fn("input_file_name", TODO_FUNCTION);
        parent.add_fn("isnan", IsNan {});
        parent.add_fn("isnull", UnaryFunction(|arg| arg.is_null()));

        parent.add_fn("monotically_increasing_id", TODO_FUNCTION);
        parent.add_fn("named_struct", TODO_FUNCTION);
        parent.add_fn("nanvl", TODO_FUNCTION);
        parent.add_fn("rand", TODO_FUNCTION);
        parent.add_fn("randn", TODO_FUNCTION);
        parent.add_fn("spark_partition_id", TODO_FUNCTION);
        parent.add_fn("when", TODO_FUNCTION);
        parent.add_fn("bitwise_not", TODO_FUNCTION);
        parent.add_fn("bitwiseNOT", TODO_FUNCTION);
        parent.add_fn("expr", SqlExpr);
        parent.add_fn("greatest", TODO_FUNCTION);
        parent.add_fn("least", TODO_FUNCTION);

        // parent.add_fn("isnan", UnaryFunction(|arg| arg.is_nan()));

        parent.add_fn("isnotnull", UnaryFunction(|arg| arg.not_null()));
        parent.add_fn("isnull", UnaryFunction(|arg| arg.is_null()));
        parent.add_fn("not", UnaryFunction(|arg| arg.not()));
    }
}

pub struct BinaryOpFunction(Operator);

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

struct SqlExpr;
impl SparkFunction for SqlExpr {
    fn to_expr(
        &self,
        args: &[Expression],
        analyzer: &SparkAnalyzer,
    ) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(|arg| analyzer.to_daft_expr(arg))
            .collect::<ConnectResult<Vec<_>>>()?;

        let [sql] = args.as_slice() else {
            invalid_argument_err!("expr requires exactly 1 argument");
        };

        let sql = sql
            .as_ref()
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| ConnectError::invalid_argument("expr argument must be a string"))?;
        Ok(sql_expr(sql)?)
    }
}
