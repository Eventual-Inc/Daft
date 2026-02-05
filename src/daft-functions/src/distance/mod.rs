use cosine::CosineDistanceFunction;
use daft_dsl::functions::FunctionModule;
use dot_product::DotProductFunction;
use euclidean::EuclideanDistanceFunction;

pub mod cosine;
pub mod dot_product;
pub mod euclidean;

pub struct DistanceFunctions;

impl FunctionModule for DistanceFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(CosineDistanceFunction);
        parent.add_fn(DotProductFunction);
        parent.add_fn(EuclideanDistanceFunction);
    }
}
