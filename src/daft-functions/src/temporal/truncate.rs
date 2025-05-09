use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Truncate {
    pub(super) interval: String,
}

#[typetag::serde]
impl ScalarUDF for Truncate {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "truncate"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input, relative_to] => {
                let input_field = input.to_field(schema)?;
                let relative_to_field = relative_to.to_field(schema)?;
                if input_field.dtype.is_temporal()
                    && (relative_to_field.dtype.is_temporal() || relative_to_field.dtype.is_null())
                {
                    Ok(Field::new(input_field.name, input_field.dtype))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expected temporal input args, got {} and {}",
                        input_field.dtype, relative_to_field.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input, relative_to] => input.dt_truncate(&self.interval, relative_to),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn dt_truncate<S: Into<String>>(input: ExprRef, interval: S, relative_to: ExprRef) -> ExprRef {
    ScalarFunction::new(
        Truncate {
            interval: interval.into(),
        },
        vec![input, relative_to],
    )
    .into()
}
