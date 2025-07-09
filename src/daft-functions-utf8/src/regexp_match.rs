use common_error::DaftResult;
use daft_core::{
    prelude::{AsArrow, BooleanArray, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_utf8_evaluate, binary_utf8_to_field, Utf8ArrayUtils};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpMatch;

#[typetag::serde]
impl ScalarUDF for RegexpMatch {
    fn name(&self) -> &'static str {
        "regexp_match"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", |s, pattern| {
            s.with_utf8_array(|arr| {
                pattern
                    .with_utf8_array(|pattern_arr| Ok(match_impl(arr, pattern_arr)?.into_series()))
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "pattern",
            DataType::is_string,
            self.name(),
            DataType::Boolean,
        )
    }

    fn docstring(&self) -> &'static str {
        "Returns true if the string matches the specified regular expression pattern"
    }
}

#[must_use]
pub fn utf8_match(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFunction::new(RegexpMatch {}, vec![input, pattern]).into()
}

fn match_impl(arr: &Utf8Array, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
    if pattern.len() == 1 {
        let pattern_scalar_value = pattern.get(0);
        return match pattern_scalar_value {
            None => Ok(BooleanArray::full_null(
                arr.name(),
                &DataType::Boolean,
                arr.len(),
            )),
            Some(pattern_v) => {
                let re = regex::Regex::new(pattern_v)?;
                let arrow_result: arrow2::array::BooleanArray = arr
                    .as_arrow()
                    .into_iter()
                    .map(|arr_v| Some(re.is_match(arr_v?)))
                    .collect();
                Ok(BooleanArray::from((arr.name(), arrow_result)))
            }
        };
    }

    arr.binary_broadcasted_compare(
        pattern,
        |data: &str, pat: &str| Ok(regex::Regex::new(pat)?.is_match(data)),
        "match",
    )
}
