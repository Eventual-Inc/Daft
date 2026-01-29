use common_error::DaftResult;
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::vector_utils::{Args, VectorMetric, compute_vector_metric, validate_vector_inputs};

fn cosine_similarity_metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;

    for (x, y) in a.iter().zip(b) {
        let x = x.to_f64()?;
        let y = y.to_f64()?;
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        return None;
    }

    Some(dot / denom)
}

struct CosineSimilarityMetric;

impl VectorMetric for CosineSimilarityMetric {
    fn metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
        cosine_similarity_metric(a, b)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CosineSimilarityFunction;

#[typetag::serde]
impl ScalarUDF for CosineSimilarityFunction {
    fn name(&self) -> &'static str {
        "cosine_similarity"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let source_name = source.name();

        let res = compute_vector_metric::<CosineSimilarityMetric>(self.name(), &source, &query)?;

        let output =
            Float64Array::from_iter(Field::new(source_name, DataType::Float64), res.into_iter());

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
pub fn cosine_similarity(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(CosineSimilarityFunction {}, vec![a, b]).into()
}
