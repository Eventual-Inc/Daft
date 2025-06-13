use std::sync::Arc;

use daft_dsl::{binary_op, null_lit, Operator};
use daft_functions::{coalesce::Coalesce, float::IsNan};
use daft_sql::sql_expr;
use spark_connect::Expression;

use super::{BinaryFunction, FunctionModule, SparkFunction, UnaryFunction};
use crate::{
    error::{ConnectError, ConnectResult},
    invalid_argument_err, not_yet_implemented,
    spark_analyzer::expr_analyzer::analyze_expr,
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
        parent.add_todo_fn("input_file_name");
        parent.add_fn("isnan", IsNan {});
        parent.add_fn("isnull", UnaryFunction(|arg| arg.is_null()));

        parent.add_todo_fn("monotically_increasing_id");
        parent.add_todo_fn("named_struct");
        parent.add_todo_fn("nanvl");
        parent.add_todo_fn("rand");
        parent.add_todo_fn("randn");
        parent.add_todo_fn("spark_partition_id");
        parent.add_fn("when", WhenFunction);
        parent.add_todo_fn("bitwise_not");
        parent.add_todo_fn("bitwiseNOT");
        parent.add_fn("expr", SqlExpr);
        parent.add_todo_fn("greatest");
        parent.add_todo_fn("least");

        // parent.add_fn("isnan", UnaryFunction(|arg| arg.is_nan()));

        parent.add_fn("isnotnull", UnaryFunction(|arg| arg.not_null()));
        parent.add_fn("isnull", UnaryFunction(|arg| arg.is_null()));
        parent.add_fn("not", UnaryFunction(|arg| arg.not()));
        parent.add_fn("and", BinaryFunction(|arg1, arg2| arg1.and(arg2)));
    }
}

pub struct WhenFunction;
impl SparkFunction for WhenFunction {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(analyze_expr)
            .collect::<ConnectResult<Vec<_>>>()?;

        let predicate = args[0].clone();

        let (if_true, if_false) = match args.len() {
            // when(predicate, then)
            2 => (args[1].clone(), null_lit()),
            // when(predicate, then).otherwise(fallback)
            3 => (args[2].clone(), args[1].clone()),

            // when(predicate, then)($.when(<predicate>, <then>)*)
            _ => {
                not_yet_implemented!("multiple .when conditions not yet supported")
            }
        };

        Ok(Arc::new(daft_dsl::Expr::IfElse {
            if_true,
            if_false,
            predicate,
        })
        .alias(args[0].name()))
    }
}

pub struct BinaryOpFunction(Operator);

impl SparkFunction for BinaryOpFunction {
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(analyze_expr)
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
    fn to_expr(&self, args: &[Expression]) -> ConnectResult<daft_dsl::ExprRef> {
        let args = args
            .iter()
            .map(analyze_expr)
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
