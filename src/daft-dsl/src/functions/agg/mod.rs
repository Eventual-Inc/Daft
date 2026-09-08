use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
    lit,
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

    fn call(
        &self,
        inputs: super::function_args::FunctionArgs<Series>,
        _ctx: &crate::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Args { input: sum, counts } = inputs.try_into()?;

        if !matches!(counts.data_type(), DataType::UInt64) {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected Counts to be type UInt64, got {}",
                counts.data_type()
            )));
        }
        match sum.data_type() {
            DataType::Decimal128(p, s) => {
                let new_type = DataType::Decimal128(*p, std::cmp::min(*p, s + Self::EXTRA_SCALE));
                let sum_array = sum.cast(&new_type)?;
                let sum_array = sum_array.decimal128()?;
                let count_array = counts.u64()?;
                Ok(sum_array.merge_mean(count_array)?.into_series())
            }
            _ => sum / counts,
        }
    }

    fn get_return_field(
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
                    Err(DaftError::TypeError(format!(
                        "Cannot infer supertypes for mean on type: {} result precision: {p_prime} exceed bounds of [1, 38]",
                        sum_field.dtype
                    )))
                } else if s_max > 38 {
                    Err(DaftError::TypeError(format!(
                        "Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed bounds of [0, 38]",
                        sum_field.dtype
                    )))
                } else if s_max > p_prime {
                    Err(DaftError::TypeError(format!(
                        "Cannot infer supertypes for mean on type: {} result scale: {s_max} exceed precision {p_prime}",
                        sum_field.dtype
                    )))
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
    ScalarFn::builtin(MergeMeanFunction {}, vec![sum, counts]).into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct MergeVarFunction;

#[derive(FunctionArgs)]
struct MergeVarArgs<T> {
    counts: T,
    sums: T,
    variances: T,
    ddof: usize,
}

#[typetag::serde]
impl ScalarUDF for MergeVarFunction {
    fn name(&self) -> &'static str {
        "merge_var"
    }

    fn call(
        &self,
        inputs: super::function_args::FunctionArgs<Series>,
        _ctx: &crate::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let MergeVarArgs {
            counts,
            sums,
            variances,
            ddof,
        } = inputs.try_into()?;

        let counts = counts.list()?;
        let sums = sums.list()?;
        let variances = variances.list()?;

        let counts_child = counts.flat_child.u64()?;
        let sums_child = sums.flat_child.f64()?;
        let variances_child = variances.flat_child.f64()?;

        let counts_offsets = counts.offsets();
        let sums_offsets = sums.offsets();
        let variances_offsets = variances.offsets();

        let ddof = ddof as f64;
        let field = Field::new(variances.name(), DataType::Float64);

        let merged = (0..counts.len()).map(|row| {
            let counts_start = counts_offsets[row] as usize;
            let counts_end = counts_offsets[row + 1] as usize;
            let sums_start = sums_offsets[row] as usize;
            let variances_start = variances_offsets[row] as usize;

            // Merge each partition's (count, mean, M2) summary with the Chan et al.
            // update. Combining partial means through their deltas keeps every
            // intermediate term on the scale of the spread, which avoids the
            // catastrophic cancellation that E(x^2) - E(x)^2 hits when the mean is
            // large relative to the variance.
            let mut total_count = 0.0f64;
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;

            for k in 0..(counts_end - counts_start) {
                let count = counts_child.get(counts_start + k).unwrap_or(0) as f64;
                // Skip empty partitions; their sum/variance are null and carry no signal.
                if count == 0.0 {
                    continue;
                }
                let (Some(sum), Some(variance)) = (
                    sums_child.get(sums_start + k),
                    variances_child.get(variances_start + k),
                ) else {
                    continue;
                };

                let partial_mean = sum / count;
                let partial_m2 = variance * count;

                if total_count == 0.0 {
                    total_count = count;
                    mean = partial_mean;
                    m2 = partial_m2;
                } else {
                    let delta = partial_mean - mean;
                    let new_count = total_count + count;
                    mean += delta * count / new_count;
                    m2 += partial_m2 + delta * delta * total_count * count / new_count;
                    total_count = new_count;
                }
            }

            (total_count > ddof).then(|| m2 / (total_count - ddof))
        });

        Ok(Float64Array::from_iter(field, merged).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let MergeVarArgs { variances, .. } = inputs.try_into()?;
        let variances_field = variances.to_field(schema)?;
        Ok(Field::new(variances_field.name, DataType::Float64))
    }
}

/// Combines per-partition `(count, sum, population_variance)` summaries into a variance with the
/// requested `ddof`, using a numerically stable merge instead of `E(x^2) - E(x)^2`.
#[must_use]
pub fn merge_var(counts: ExprRef, sums: ExprRef, variances: ExprRef, ddof: usize) -> ExprRef {
    ScalarFn::builtin(
        MergeVarFunction {},
        vec![counts, sums, variances, lit(ddof as u64)],
    )
    .into()
}
