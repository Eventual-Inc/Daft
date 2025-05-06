use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{AsArrow, DataType, Field, Schema, UInt64Array, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LengthBytes;

#[typetag::serde]
impl ScalarUDF for LengthBytes {
    fn name(&self) -> &'static str {
        "length_bytes"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, "length_bytes requires exactly 1 input");

        let input = inputs.required((0, "input"))?;
        if input.data_type().is_null() {
            return Ok(Series::full_null(
                input.name(),
                &DataType::Null,
                input.len(),
            ));
        }
        series_length_bytes(input)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required((0, "input"))?.to_field(schema)?;

        if input.dtype.is_null() {
            Ok(Field::new(input.name, DataType::Null))
        } else {
            ensure!(
                input.dtype.is_string(),
                TypeError: "Expects input to length_bytes to be utf8, but received {}", input.dtype
            );

            Ok(Field::new(input.name, DataType::UInt64))
        }
    }
    fn docstring(&self) -> &'static str {
        "Returns the length of the string in bytes"
    }
}

#[must_use]
pub fn utf8_length_bytes(input: ExprRef) -> ExprRef {
    ScalarFunction::new(LengthBytes {}, vec![input]).into()
}

pub fn series_length_bytes(s: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|arr| Ok(length_bytes_impl(arr)?.into_series()))
}

fn length_bytes_impl(arr: &Utf8Array) -> DaftResult<UInt64Array> {
    let self_arrow = arr.as_arrow();
    let arrow_result = self_arrow
        .iter()
        .map(|val| {
            let v = val?;
            Some(v.len() as u64)
        })
        .collect::<arrow2::array::UInt64Array>()
        .with_validity(self_arrow.validity().cloned());
    Ok(UInt64Array::from((arr.name(), Box::new(arrow_result))))
}
