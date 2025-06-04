use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct MergeMeanFunction;

impl MergeMeanFunction {
    const EXTRA_SCALE: usize = 4;
}
#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    counts: T,
}

#[typetag::serde]
impl ScalarUDF for MergeMeanFunction {
    fn name(&self) -> &'static str {
        "merge_mean"
    }

    fn call_with_args(
        &self,
        inputs: super::function_args::FunctionArgs<Series>,
    ) -> DaftResult<Series> {
        let inputs = inputs.into_inner();

        self.call(&inputs)
    }

    fn get_return_type_from_args(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input: sum, counts } = inputs.try_into()?;
        let count_field = counts.to_field(schema)?;
        if !matches!(count_field.dtype, DataType::UInt64) {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected Counts to be type UInt64, got {}",
                count_field.dtype
            )));
        }

        let sum_field = sum.to_field(schema)?;
        match sum_field.dtype {
            DataType::Decimal128(p, s) => {
                let p_prime = p;

                let s_max = std::cmp::min(p_prime, s + Self::EXTRA_SCALE);

                if !(1..=38).contains(&p_prime) {
                    Err(DaftError::TypeError(
                        format!("Cannot infer supertypes for mean on type: {} result precision: {p_prime} exceed bounds of [1, 38]", sum_field.dtype)
                    ))
                } else if s_max > 38 {
                    Err(DaftError::TypeError(
                        format!("Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed bounds of [0, 38]", sum_field.dtype)
                    ))
                } else if s_max > p_prime {
                    Err(DaftError::TypeError(
                        format!("Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed precision {p_prime}", sum_field.dtype)
                    ))
                } else {
                    Ok(Field::new(
                        sum_field.name,
                        DataType::Decimal128(p_prime, s_max),
                    ))
                }
            }
            _ => sum.div(counts).to_field(schema),
        }
    }
}

#[must_use]
pub fn merge_mean(sum: ExprRef, counts: ExprRef) -> ExprRef {
    ScalarFunction::new(MergeMeanFunction {}, vec![sum, counts]).into()
}
