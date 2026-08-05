use daft_dsl::functions::FunctionModule;

pub mod great_circle_distance;

use great_circle_distance::GreatCircleDistance;

pub struct SpatialFunctions;

impl FunctionModule for SpatialFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(GreatCircleDistance);
    }
}
