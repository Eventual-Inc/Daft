use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BooleanArray, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{
    binary_utf8_evaluate, binary_utf8_to_field, create_broadcasted_str_iter, parse_inputs,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ILike;

#[typetag::serde]
impl ScalarUDF for ILike {
    fn name(&self) -> &'static str {
        "ilike"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", |s, pattern| {
            s.with_utf8_array(|arr| {
                pattern
                    .with_utf8_array(|pattern_arr| Ok(ilike_impl(arr, pattern_arr)?.into_series()))
            })
        })
    }

    fn function_args_to_field(
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
        "Returns a boolean indicating whether string matches the given pattern. (case-insensitive)"
    }
}

#[must_use]
pub fn ilike(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFunction::new(ILike {}, vec![input, pattern]).into()
}

fn ilike_impl(arr: &Utf8Array, pattern: &Utf8Array) -> DaftResult<BooleanArray> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[pattern])
        .map_err(|e| DaftError::ValueError(format!("Error in ilike: {e}")))?;
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
            let re = regex::Regex::new(&format!("(?i)^{}$", pat));
            let re = re?;
            self_iter
                .map(|val| Some(re.is_match(val?)))
                .collect::<arrow2::array::BooleanArray>()
        }
        _ => {
            let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
            self_iter
                .zip(pattern_iter)
                .map(|(val, pat)| match (val, pat) {
                    (Some(val), Some(pat)) => {
                        let pat = pat.replace('%', ".*").replace('_', ".");
                        let re = regex::Regex::new(&format!("(?i)^{}$", pat));
                        Ok(Some(re?.is_match(val)))
                    }
                    _ => Ok(None),
                })
                .collect::<DaftResult<arrow2::array::BooleanArray>>()?
        }
    };

    let result = BooleanArray::from((arr.name(), Box::new(arrow_result)));
    assert_eq!(result.len(), expected_size);
    Ok(result)
}
