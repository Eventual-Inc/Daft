use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
    utils::supertype::try_get_supertype,
};
use daft_dsl::{
    ExprRef,
    functions::{prelude::*, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use super::NotNan;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FillNan;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    fill_value: T,
}

#[typetag::serde]
impl ScalarUDF for FillNan {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, fill_value } = inputs.try_into()?;

        if input.data_type() == &DataType::Null {
            return Ok(Series::full_null(
                input.name(),
                input.data_type(),
                input.len(),
            ));
        }

        // TODO(perf): we can likely do this without fully evaluating the not_nan predicate first
        // The original implementation also did this, but was hidden behind the series methods.
        let args = FunctionArgs::new_unnamed(vec![input.clone()]);

        let predicate = NotNan {}.call(args)?;
        match fill_value.len() {
            1 => {
                let fill_value = fill_value.broadcast(input.len())?;
                input.if_else(&fill_value, &predicate)
            }
            len if len == input.len() => input.if_else(&fill_value, &predicate),
            len => Err(DaftError::ValueError(format!(
                "Expected fill_value to be a scalar or a vector of the same length as input, but received {len} and {}",
                input.len()
            ))),
        }
    }

    fn name(&self) -> &'static str {
        "fill_nan"
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, fill_value } = inputs.try_into()?;
        let data_field = input.to_field(schema)?;
        let fill_value_field = fill_value.to_field(schema)?;
        if data_field.dtype == DataType::Null {
            Ok(Field::new(data_field.name, DataType::Null))
        } else {
            match (
                &data_field.dtype.is_floating(),
                &fill_value_field.dtype.is_floating(),
                try_get_supertype(&data_field.dtype, &fill_value_field.dtype),
            ) {
                (true, true, Ok(dtype)) => Ok(Field::new(data_field.name, dtype)),
                _ => Err(DaftError::TypeError(format!(
                    "Expects input for fill_nan to be float, but received {data_field} and {fill_value_field}",
                ))),
            }
        }
    }

    fn docstring(&self) -> &'static str {
        "Replaces NaN values in the input expression with a specified fill value."
    }
}

#[must_use]
pub fn fill_nan(input: ExprRef, fill_value: ExprRef) -> ExprRef {
    ScalarFn::builtin(FillNan {}, vec![input, fill_value]).into()
}
