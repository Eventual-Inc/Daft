use common_error::DaftResult;
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::vector_utils::{Args, VectorMetric, compute_vector_metric, validate_vector_inputs};

fn pearson_correlation_metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
    if a.is_empty() {
        return None;
    }

    let n = a.len() as f64;
    let mean_a = a.iter().map(|x| x.to_f64()).sum::<Option<f64>>()? / n;
    let mean_b = b.iter().map(|x| x.to_f64()).sum::<Option<f64>>()? / n;

    let mut numerator = 0.0;
    let mut denom_a = 0.0;
    let mut denom_b = 0.0;

    for (x, y) in a.iter().zip(b) {
        let dx = x.to_f64()? - mean_a;
        let dy = y.to_f64()? - mean_b;
        numerator += dx * dy;
        denom_a += dx * dx;
        denom_b += dy * dy;
    }

    let denom = (denom_a * denom_b).sqrt();
    if denom == 0.0 {
        return None;
    }

    Some(numerator / denom)
}

struct PearsonCorrelationMetric;

impl VectorMetric for PearsonCorrelationMetric {
    fn metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
        pearson_correlation_metric(a, b)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PearsonCorrelationFunction;

#[typetag::serde]
impl ScalarUDF for PearsonCorrelationFunction {
    fn name(&self) -> &'static str {
        "pearson_correlation"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let output =
            compute_vector_metric::<PearsonCorrelationMetric>(self.name(), &source, &query)?;

        Ok(output.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let source = source.to_field(schema)?;
        let query = query.to_field(schema)?;

        validate_vector_inputs(self.name(), &source, &query)
    }
}

#[must_use]
pub fn pearson_correlation(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(PearsonCorrelationFunction {}, vec![a, b]).into()
}
