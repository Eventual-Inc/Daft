use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{BooleanArray, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Like;

#[typetag::serde]
impl ScalarUDF for Like {
    fn name(&self) -> &'static str {
        "like"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        series_like(input, pattern)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, SchemaMismatch: "like expects 2 arguments");

        let input = inputs.required((0, "input"))?.to_field(schema)?;

        ensure!(input.dtype == DataType::Utf8, TypeError: "input must be of type Utf8");

        let pattern = inputs.required((1, "pattern"))?.to_field(schema)?;
        ensure!(
            pattern.dtype == DataType::Utf8,
            TypeError: "pattern must be of type Utf8"
        );
        Ok(Field::new(input.name, DataType::Boolean))
    }
}

#[must_use]
pub fn like(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFunction::new(Like {}, vec![input, pattern]).into()
}

pub fn series_like(s: &Series, pattern: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        pattern.with_utf8_array(|pattern_arr| Ok(like_impl(arr, pattern_arr)?.into_series()))
    })
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
                .collect::<arrow2::array::BooleanArray>()
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
                .collect::<DaftResult<arrow2::array::BooleanArray>>()?
        }
    };
    let result = BooleanArray::from((arr.name(), Box::new(arrow_result)));
    assert_eq!(result.len(), expected_size);
    Ok(result)
}
