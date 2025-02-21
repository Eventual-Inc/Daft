use daft_dsl::functions::partitioning;
use spark_connect::Expression;

use super::{FunctionModule, SparkFunction, UnaryFunction};
use crate::{
    error::{ConnectError, ConnectResult},
    invalid_argument_err,
    spark_analyzer::ExprResolver,
};

// https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#partition-transformation-functions
pub struct PartitionTransformFunctions;

impl FunctionModule for PartitionTransformFunctions {
    fn register(parent: &mut super::SparkFunctions) {
        parent.add_fn("years", UnaryFunction(partitioning::years));
        parent.add_fn("months", UnaryFunction(partitioning::months));
        parent.add_fn("days", UnaryFunction(partitioning::days));
        parent.add_fn("hours", UnaryFunction(partitioning::hours));
        parent.add_fn("bucket", BucketFunction);
    }
}

struct BucketFunction;

impl SparkFunction for BucketFunction {
    fn to_expr(
        &self,
        args: &[Expression],
        expr_resolver: &ExprResolver,
    ) -> ConnectResult<daft_dsl::ExprRef> {
        match args {
            [n_buckets, arg] => {
                let n_buckets = expr_resolver.resolve_expr(n_buckets)?;
                let arg = expr_resolver.resolve_expr(arg)?;

                let n_buckets = n_buckets
                    .as_literal()
                    .and_then(|lit| lit.as_i32())
                    .ok_or_else(|| {
                        ConnectError::invalid_argument("first argument must be an integer")
                    })?;

                Ok(partitioning::iceberg_bucket(arg, n_buckets))
            }
            _ => invalid_argument_err!("requires exactly two arguments"),
        }
    }
}
