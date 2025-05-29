use daft_dsl::functions::{FunctionModule, FunctionRegistry};

mod json_loads;
mod json_query;

/// JsonFunctions module.
pub struct JsonFunctions;

/// JsonFunctions module registration.
impl FunctionModule for JsonFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(crate::json_loads::JsonLoads);
        parent.add_fn(crate::json_query::JsonQuery);
    }
}
