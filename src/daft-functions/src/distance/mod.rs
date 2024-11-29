mod cosine;

use cosine::CosineDistanceFunction;
use daft_dsl::{functions::ScalarFunction, ExprRef};

#[must_use]
pub fn cosine_distance(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFunction::new(CosineDistanceFunction {}, vec![a, b]).into()
}
