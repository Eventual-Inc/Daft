use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::Expr;

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;

macro_rules! impl_func_evaluator_for_partitioning {
    ($name:ident, $op:ident, $kernel:ident, $result_type:ident) => {
        pub(super) struct $name {}

        impl FunctionEvaluator for $name {
            fn fn_name(&self) -> &'static str {
                stringify!($op)
            }

            fn to_field(&self, inputs: &[Expr], schema: &Schema, _: &Expr) -> DaftResult<Field> {
                match inputs {
                    [input] => match input.to_field(schema) {
                        Ok(field) if field.dtype.is_temporal() => {
                            Ok(Field::new(field.name, $result_type))
                        }
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

            fn evaluate(&self, inputs: &[Series], _: &Expr) -> DaftResult<Series> {
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
use DataType::{Int32, Date};
impl_func_evaluator_for_partitioning!(YearsEvaluator, years, partitioning_years, Int32);
impl_func_evaluator_for_partitioning!(MonthsEvaluator, months, partitioning_months, Int32);
impl_func_evaluator_for_partitioning!(DaysEvaluator, days, partitioning_days, Date);
impl_func_evaluator_for_partitioning!(HoursEvaluator, hours, partitioning_hours, Int32);
