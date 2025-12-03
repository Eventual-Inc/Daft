use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BooleanArray, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{
    binary_utf8_evaluate, binary_utf8_to_field, create_broadcasted_str_iter, parse_inputs,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Like;

#[typetag::serde]
impl ScalarUDF for Like {
    fn name(&self) -> &'static str {
        "like"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", |s, pattern| {
            s.with_utf8_array(|arr| {
                pattern
                    .with_utf8_array(|pattern_arr| Ok(like_impl(arr, pattern_arr)?.into_series()))
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
        "Returns a boolean indicating whether string matches the given pattern. (case-sensitive)"
    }
}

#[must_use]
pub fn like(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFn::builtin(Like {}, vec![input, pattern]).into()
}

fn like_impl(arr: &Utf8Array, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[pattern])
        .map_err(|e| DaftError::ValueError(format!("Error in like: {e}")))?;
    if is_full_null {
        return Ok(BooleanArray::full_null(
            arr.name(),
            &DataType::Boolean,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(BooleanArray::empty(arr.name(), &DataType::Boolean));
    }

    let self_iter = create_broadcasted_str_iter(arr, expected_size);
    let arrow_result = match pattern.len() {
        1 => {
            let pat = pattern.get(0).unwrap();
            let pat = pat.replace('%', ".*").replace('_', ".");
            let re = regex::Regex::new(&format!("^{}$", pat));
            let re = re?;
            self_iter
                .map(|val| Some(re.is_match(val?)))
                .collect::<daft_arrow::array::BooleanArray>()
        }
        _ => {
            let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
            self_iter
                .zip(pattern_iter)
                .map(|(val, pat)| match (val, pat) {
                    (Some(val), Some(pat)) => {
                        let pat = pat.replace('%', ".*").replace('_', ".");
                        let re = regex::Regex::new(&format!("^{}$", pat));
                        Ok(Some(re?.is_match(val)))
                    }
                    _ => Ok(None),
                })
                .collect::<DaftResult<daft_arrow::array::BooleanArray>>()?
        }
    };
    let result = BooleanArray::from((arr.name(), Box::new(arrow_result)));
    assert_eq!(result.len(), expected_size);
    Ok(result)
}
