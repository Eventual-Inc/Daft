use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Capitalize;

#[typetag::serde]
impl ScalarUDF for Capitalize {
    fn name(&self) -> &'static str {
        "capitalize"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        series_capitalize(input)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(
            input.dtype == DataType::Utf8,
            TypeError: "Expects input to capitalize to be utf8, but received {}", input.dtype
        );

        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Capitalize a UTF-8 string."
    }
}

pub fn capitalize(e: ExprRef) -> ExprRef {
    ScalarFunction::new(Capitalize, vec![e]).into()
}
pub fn series_capitalize(s: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|u| {
        u.unary_broadcasted_op(|val| {
            let mut chars = val.chars();
            match chars.next() {
                None => "".into(),
                Some(first) => {
                    let first_char_uppercased = first.to_uppercase();
                    let mut res = String::with_capacity(val.len());
                    res.extend(first_char_uppercased);
                    res.extend(chars.flat_map(|c| c.to_lowercase()));
                    res.into()
                }
            }
        })
        .map(IntoSeries::into_series)
    })
}
