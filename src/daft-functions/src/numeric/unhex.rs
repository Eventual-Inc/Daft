use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BinaryArray, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Unhex;

#[derive(FunctionArgs)]
struct UnhexArgs<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for Unhex {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnhexArgs { input } = inputs.try_into()?;

        let input_utf8 = input.cast(&daft_core::datatypes::DataType::Utf8)?;
        let input_array = input_utf8.downcast::<Utf8Array>()?;

        let bytes: Vec<Option<Vec<u8>>> = input_array
            .iter()
            .map(|opt_str| match opt_str {
                Some(hex_str) => {
                    let hex_str = hex_str.trim_start_matches("0x");
                    if hex_str.len() % 2 != 0 {
                        return None;
                    }
                    hex::decode(hex_str).ok()
                }
                None => None,
            })
            .collect();

        let binary_array = BinaryArray::from_iter(input.name(), bytes);

        Ok(binary_array.into_series())
    }

    fn name(&self) -> &'static str {
        "unhex"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required(0)?;
        let field = input.to_field(schema)?;

        if !field.dtype.is_string() {
            return Err(DaftError::TypeError(format!(
                "Expected input to unhex to be string, got {}",
                field.dtype
            )));
        }

        Ok(Field::new(
            field.name,
            daft_core::datatypes::DataType::Binary,
        ))
    }

    fn docstring(&self) -> &'static str {
        "Converts a hex string to bytes."
    }
}

#[must_use]
pub fn unhex(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Unhex {}, vec![input]).into()
}
