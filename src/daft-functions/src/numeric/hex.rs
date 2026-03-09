use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema, UInt64Array, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{scalar::ScalarFn, FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Hex;

#[derive(FunctionArgs)]
struct HexArgs<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for Hex {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let HexArgs { input } = inputs.try_into()?;

        let input_u64 = input.cast(&daft_core::datatypes::DataType::UInt64)?;
        let input_array = input_u64.downcast::<UInt64Array>()?;

        let hex_strings: Vec<String> = input_array
            .iter()
            .map(|opt_val| match opt_val {
                Some(val) => format!("0x{:x}", val),
                None => "".to_string(),
            })
            .collect();

        let utf8_array = Utf8Array::from_values(input.name(), hex_strings);

        Ok(utf8_array.into_series())
    }

    fn name(&self) -> &'static str {
        "hex"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required(0)?;
        let field = input.to_field(schema)?;

        if !field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected input to hex to be numeric, got {}",
                field.dtype
            )));
        }

        Ok(Field::new(field.name, daft_core::datatypes::DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Computes hex value of the given column."
    }
}

#[must_use]
pub fn hex(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Hex {}, vec![input]).into()
}