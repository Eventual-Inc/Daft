use common_error::DaftResult;
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::vector_utils::{Args, VectorMetric, compute_vector_metric, validate_vector_inputs};

fn cosine_distance_metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
    let mut xy = 0.0;
    let mut x_sq = 0.0;
    let mut y_sq = 0.0;

    for (x, y) in a.iter().zip(b) {
        let x = x.to_f64()?;
        let y = y.to_f64()?;
        xy += x * y;
        x_sq += x * x;
        y_sq += y * y;
    }

    Some(1.0 - xy / (x_sq.sqrt() * y_sq.sqrt()))
}

struct CosineDistanceMetric;

impl VectorMetric for CosineDistanceMetric {
    fn metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
        cosine_distance_metric(a, b)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CosineDistanceFunction;

#[typetag::serde]
impl ScalarUDF for CosineDistanceFunction {
    fn name(&self) -> &'static str {
        "cosine_distance"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let output = compute_vector_metric::<CosineDistanceMetric>(self.name(), &source, &query)?;

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
pub fn cosine_distance(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(CosineDistanceFunction {}, vec![a, b]).into()
}
