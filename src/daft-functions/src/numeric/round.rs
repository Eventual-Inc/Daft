use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::{evaluate_single_numeric, to_field_single_numeric};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Round {
    decimal: i32,
}

#[typetag::serde]
impl ScalarUDF for Round {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "round"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if self.decimal < 0 {
            return Err(DaftError::ComputeError(format!(
                "decimal value can not be negative: {}",
                self.decimal
            )));
        }
        evaluate_single_numeric(inputs, |s| match s.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(s.clone()),
            DataType::Float32 => Ok(s.f32().unwrap().round(self.decimal)?.into_series()),
            DataType::Float64 => Ok(s.f64().unwrap().round(self.decimal)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "round not implemented for {}",
                dt
            ))),
        })
    }
}

#[must_use]
pub fn round(input: ExprRef, decimal: Option<i32>) -> ExprRef {
    ScalarFunction::new(
        Round {
            decimal: decimal.unwrap_or_default(),
        },
        vec![input],
    )
    .into()
}
