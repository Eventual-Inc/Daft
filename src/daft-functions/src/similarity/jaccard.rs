use common_error::DaftResult;
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::vector_utils::{Args, VectorMetric, compute_vector_metric, validate_vector_inputs};

fn jaccard_similarity_metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
    let mut intersection = 0.0;
    let mut union = 0.0;

    for (x, y) in a.iter().zip(b) {
        let x_nonzero = !x.is_zero();
        let y_nonzero = !y.is_zero();
        if x_nonzero || y_nonzero {
            union += 1.0;
            if x_nonzero && y_nonzero {
                intersection += 1.0;
            }
        }
    }

    if union == 0.0 {
        return Some(1.0);
    }

    Some(intersection / union)
}

struct JaccardSimilarityMetric;

impl VectorMetric for JaccardSimilarityMetric {
    fn metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
        jaccard_similarity_metric(a, b)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JaccardSimilarityFunction;

#[typetag::serde]
impl ScalarUDF for JaccardSimilarityFunction {
    fn name(&self) -> &'static str {
        "jaccard_similarity"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let output =
            compute_vector_metric::<JaccardSimilarityMetric>(self.name(), &source, &query)?;

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
pub fn jaccard_similarity(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(JaccardSimilarityFunction {}, vec![a, b]).into()
}
