use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_utf8_evaluate, unary_utf8_to_field, Utf8ArrayUtils};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Capitalize;

#[typetag::serde]
impl ScalarUDF for Capitalize {
    fn name(&self) -> &'static str {
        "capitalize"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, capitalize_impl)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
    }

    fn docstring(&self) -> &'static str {
        "Capitalize a UTF-8 string."
    }
}

pub fn capitalize(e: ExprRef) -> ExprRef {
    ScalarFunction::new(Capitalize, vec![e]).into()
}

fn capitalize_impl(s: &Series) -> DaftResult<Series> {
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
