use cosine::CosineDistanceFunction;
use daft_dsl::functions::FunctionModule;

pub mod cosine;

pub struct DistanceFunctions;

impl FunctionModule for DistanceFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(CosineDistanceFunction);
    }
}
