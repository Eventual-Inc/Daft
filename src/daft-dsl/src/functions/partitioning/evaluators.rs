use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::{functions::partitioning::PartitioningExpr, ExprRef};

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

macro_rules! impl_func_evaluator_for_partitioning {
    ($name:ident, $op:ident, $kernel:ident, $result_type:ident) => {
        pub(super) struct $name {}

        impl FunctionEvaluator for $name {
            fn fn_name(&self) -> &'static str {
                stringify!($op)
            }

            fn to_field(
                &self,
                inputs: &[ExprRef],
                schema: &Schema,
                _: &FunctionExpr,
            ) -> DaftResult<Field> {
                match inputs {
                    [input] => match input.to_field(schema) {
                        Ok(field) if field.dtype.is_temporal() => Ok(Field::new(
                            format!("{}_{}", field.name, stringify!($op)),
                            $result_type,
                        )),
                        Ok(field) => Err(DaftError::TypeError(format!(
                            "Expected input to {} to be temporal, got {}",
                            stringify!($op),
                            field.dtype
                        ))),
                        Err(e) => Err(e),
                    },
                    _ => Err(DaftError::SchemaMismatch(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    ))),
                }
            }

            fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
                match inputs {
                    [input] => input.$kernel(),
                    _ => Err(DaftError::ValueError(format!(
                        "Expected 1 input arg for {}, got {}",
                        stringify!($op),
                        inputs.len()
                    ))),
                }
            }
        }
    };
}
use crate::functions::FunctionExpr;
use DataType::{Date, Int32};
impl_func_evaluator_for_partitioning!(YearsEvaluator, years, partitioning_years, Int32);
impl_func_evaluator_for_partitioning!(MonthsEvaluator, months, partitioning_months, Int32);
impl_func_evaluator_for_partitioning!(DaysEvaluator, days, partitioning_days, Date);
impl_func_evaluator_for_partitioning!(HoursEvaluator, hours, partitioning_hours, Int32);

pub(super) struct IcebergBucketEvaluator {}

impl FunctionEvaluator for IcebergBucketEvaluator {
    fn fn_name(&self) -> &'static str {
        "partitioning_iceberg_bucket"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => match input.to_field(schema) {
                Ok(field) => match field.dtype {
                    DataType::Decimal128(_, _)
                    | DataType::Date
                    | DataType::Timestamp(..)
                    | DataType::Utf8
                    | DataType::Binary => Ok(Field::new(
                        format!("{}_bucket", field.name),
                        DataType::Int32,
                    )),
                    v if v.is_integer() => Ok(Field::new(
                        format!("{}_bucket", field.name),
                        DataType::Int32,
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to iceberg bucketing to be murmur3 hashable, got {}",
                        field.dtype
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let n = match expr {
            FunctionExpr::Partitioning(PartitioningExpr::IcebergBucket(n)) => n,
            _ => panic!("Expected PartitioningExpr::IcebergBucket Expr, got {expr}"),
        };

        match inputs {
            [input] => input.partitioning_iceberg_bucket(*n),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub(super) struct IcebergTruncateEvaluator {}

impl FunctionEvaluator for IcebergTruncateEvaluator {
    fn fn_name(&self) -> &'static str {
        "partitioning_iceberg_truncate"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [input] => match input.to_field(schema) {
                Ok(field) => match &field.dtype {
                    DataType::Decimal128(_, _)
                    | DataType::Utf8 => Ok(Field::new(format!("{}_truncate", field.name), field.dtype)),
                    v if v.is_integer() => Ok(Field::new(format!("{}_truncate", field.name), field.dtype)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to IcebergTruncate to be an Integer, Utf8 or Decimal, got {}",
                        field.dtype
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        let w = match expr {
            FunctionExpr::Partitioning(PartitioningExpr::IcebergTruncate(w)) => w,
            _ => panic!("Expected PartitioningExpr::IcebergTruncate Expr, got {expr}"),
        };

        match inputs {
            [input] => input.partitioning_iceberg_truncate(*w),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
