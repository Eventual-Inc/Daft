use common_error::DaftResult;
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::vector_utils::{Args, VectorMetric, compute_vector_metric, validate_vector_inputs};

fn euclidean_distance_metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
    let sum_sq = a
        .iter()
        .zip(b)
        .map(|(x, y)| {
            let dx = x.to_f64()? - y.to_f64()?;
            Some(dx * dx)
        })
        .sum::<Option<f64>>()?;
    Some(sum_sq.sqrt())
}

struct EuclideanDistanceMetric;

impl VectorMetric for EuclideanDistanceMetric {
    fn metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
        euclidean_distance_metric(a, b)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EuclideanDistanceFunction;

#[typetag::serde]
impl ScalarUDF for EuclideanDistanceFunction {
    fn name(&self) -> &'static str {
        "euclidean_distance"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let output =
            compute_vector_metric::<EuclideanDistanceMetric>(self.name(), &source, &query)?;

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
pub fn euclidean_distance(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(EuclideanDistanceFunction {}, vec![a, b]).into()
}
