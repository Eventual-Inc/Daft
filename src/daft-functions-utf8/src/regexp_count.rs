use std::sync::Arc;

use common_error::{DaftError, DaftResult, ensure};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    patterns: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpCount;

#[typetag::serde]
impl ScalarUDF for RegexpCount {
    fn name(&self) -> &'static str {
        "regexp_count"
    }
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, patterns } = inputs.try_into()?;

        let input = input
            .utf8()
            .map_err(|_| DaftError::InternalError("Expected input to be a string".to_string()))?;
        let patterns = patterns.utf8().map_err(|_| {
            DaftError::InternalError("Expected patterns to be a string".to_string())
        })?;

        Ok(regexp_count_impl(input, patterns)?.into_series())
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let Args { input, patterns } = args.try_into()?;
        let patterns = patterns.to_field(schema)?;
        ensure!(patterns.dtype == DataType::Utf8, TypeError: "Expected patterns to be a string");
        let input = input.to_field(schema)?;
        ensure!(input.dtype == DataType::Utf8, TypeError: "Expected input to be a string");

        Ok(Field::new(input.name, DataType::UInt64))
    }

    fn docstring(&self) -> &'static str {
        "Count the number of regex matches for each string in the input array."
    }
}

#[must_use]
pub fn regexp_count(input: ExprRef, patterns: ExprRef) -> ExprRef {
    ScalarFn::builtin(RegexpCount, vec![input, patterns]).into()
}

fn regexp_count_impl(arr: &Utf8Array, patterns: &Utf8Array) -> DaftResult<UInt64Array> {
    if patterns.len() == 1 {
        // Single pattern case
        let pattern_str = patterns
            .get(0)
            .ok_or_else(|| DaftError::ValueError("Pattern cannot be null".to_string()))?;

        let regex = regex::Regex::new(pattern_str)
            .map_err(|e| DaftError::ValueError(format!("Invalid regex pattern: {}", e)))?;

        let iter = arr
            .as_arrow()
            .iter()
            // Note, this is optimized by the compiler to not materialize values
            .map(|opt| opt.map(|s| regex.find_iter(s).count() as u64));

        Ok(UInt64Array::from_iter(
            Arc::new(Field::new(arr.name(), DataType::UInt64)),
            iter,
        ))
    } else {
        let res = arr
            .as_arrow()
            .iter()
            .zip(patterns.as_arrow().iter())
            .map(|(val, pat)| {
                let Some(val) = val else { return Ok(None) };
                let Some(pat) = pat else { return Ok(None) };

                let regex = regex::Regex::new(pat)
                    .map_err(|e| DaftError::ValueError(format!("Invalid regex pattern: {}", e)))?;

                Ok(Some(regex.find_iter(val).count() as u64))
            })
            .collect::<DaftResult<arrow2::array::UInt64Array>>()?;

        Ok(UInt64Array::from((arr.name(), Box::new(res))))
    }
}
