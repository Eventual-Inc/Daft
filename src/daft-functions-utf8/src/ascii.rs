use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_utf8_evaluate, unary_utf8_to_field};

/// Spark-compatible `ascii` function.
/// Returns the ASCII numeric value of the first character of the string.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Ascii;

#[typetag::serde]
impl ScalarUDF for Ascii {
    fn name(&self) -> &'static str {
        "ascii"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| {
                let result: daft_core::array::DataArray<daft_core::prelude::Int32Type> = arr
                    .into_iter()
                    .map(|val| val.map(|v| v.bytes().next().map(|b| b as i32).unwrap_or(0)))
                    .collect::<daft_core::array::DataArray<daft_core::prelude::Int32Type>>()
                    .rename(arr.name());
                Ok(result.into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Int32)
    }

    fn docstring(&self) -> &'static str {
        "Returns the ASCII numeric value of the first character of the string. Returns 0 for empty strings."
    }
}

#[must_use]
pub fn ascii(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Ascii {}, vec![input]).into()
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::Utf8Array;

    #[test]
    fn test_ascii_basic() {
        let arr = Utf8Array::from_iter(
            "a",
            vec![Some("A"), Some("abc"), Some(""), None].into_iter(),
        );

        let results: Vec<Option<i32>> = arr
            .into_iter()
            .map(|val| val.map(|v| v.bytes().next().map(|b| b as i32).unwrap_or(0)))
            .collect();

        assert_eq!(results[0], Some(65)); // 'A'
        assert_eq!(results[1], Some(97)); // 'a'
        assert_eq!(results[2], Some(0)); // empty string
        assert_eq!(results[3], None); // null
    }
}
