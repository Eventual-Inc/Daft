use common_error::DaftResult;
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::vector_utils::{Args, VectorMetric, compute_vector_metric, validate_vector_inputs};

fn dot_product_metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
    let dot = a
        .iter()
        .zip(b)
        .map(|(x, y)| Some(x.to_f64()? * y.to_f64()?))
        .sum::<Option<f64>>()?;
    Some(dot)
}

struct DotProductMetric;

impl VectorMetric for DotProductMetric {
    fn metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64> {
        dot_product_metric(a, b)
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DotProductFunction;

#[typetag::serde]
impl ScalarUDF for DotProductFunction {
    fn name(&self) -> &'static str {
        "dot_product"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let source_name = source.name();

        let res = compute_vector_metric::<DotProductMetric>(self.name(), &source, &query)?;

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
pub fn dot_product(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(DotProductFunction {}, vec![a, b]).into()
}
