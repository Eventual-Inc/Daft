use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
    utils::supertype::try_get_supertype,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::NotNan;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FillNan;

#[typetag::serde]
impl ScalarUDF for FillNan {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, ComputeError: "Expected 2 input args, got {}", inputs.len());
        let data = inputs.required((0, "input"))?;
        let fill_value = inputs.required((1, "fill_value"))?;

        if data.data_type() == &DataType::Null {
            return Ok(Series::full_null(data.name(), data.data_type(), data.len()));
        }

        // TODO(perf): we can likely do this without fully evaluating the not_nan predicate first
        // The original implementation also did this, but was hidden behind the series methods.
        let predicate = NotNan {}.evaluate_from_series(&[data.clone()])?;
        match fill_value.len() {
            1 => {
                let fill_value = fill_value.broadcast(data.len())?;
                data.if_else(&fill_value, &predicate)

            }
            len if len == data.len() => {
                data.if_else(fill_value, &predicate)

            }
            len => Err(DaftError::ValueError(format!(
                "Expected fill_value to be a scalar or a vector of the same length as data, but received {len} and {}",
                data.len()
            )))
        }
    }

    fn name(&self) -> &'static str {
        "fill_nan"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, fill_value] => match (data.to_field(schema), fill_value.to_field(schema)) {
                (Ok(data_field), Ok(fill_value_field)) => {
                    if data_field.dtype == DataType::Null {
                        Ok(Field::new(data_field.name, DataType::Null))
                    } else {
                        match (&data_field.dtype.is_floating(), &fill_value_field.dtype.is_floating(), try_get_supertype(&data_field.dtype, &fill_value_field.dtype)) {
                        (true, true, Ok(dtype)) => Ok(Field::new(data_field.name, dtype)),
                        _ => Err(DaftError::TypeError(format!(
                            "Expects input for fill_nan to be float, but received {data_field} and {fill_value_field}",
                        ))),
                    }
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Replaces NaN values in the input expression with a specified fill value."
    }
}

#[must_use]
pub fn fill_nan(input: ExprRef, fill_value: ExprRef) -> ExprRef {
    ScalarFunction::new(FillNan {}, vec![input, fill_value]).into()
}
