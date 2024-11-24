use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct MergeMeanFunction;

impl MergeMeanFunction {
    const EXTRA_SCALE: usize = 4;
}

#[typetag::serde]
impl ScalarUDF for MergeMeanFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "merge_mean"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [sum, counts] => {
                if !matches!(counts.data_type(), DataType::UInt64) {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Expected Counts to be type UInt64, got {}",
                        counts.data_type()
                    )));
                }
                match sum.data_type() {
                    DataType::Decimal128(p, s) => {
                        let new_type =
                            DataType::Decimal128(*p, std::cmp::min(*p, s + Self::EXTRA_SCALE));
                        let sum_array = sum.cast(&new_type)?;
                        let sum_array = sum_array.decimal128()?;
                        let count_array = counts.u64()?;
                        Ok(sum_array.merge_mean(count_array)?.into_series())
                    }
                    _ => sum / counts,
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [sum, counts] => {
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
                    _ => sum.clone().div(counts.clone()).to_field(schema),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn merge_mean(sum: ExprRef, counts: ExprRef) -> ExprRef {
    ScalarFunction::new(MergeMeanFunction {}, vec![sum, counts]).into()
}
