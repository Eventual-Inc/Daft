use daft_dsl::functions::{FunctionModule, FunctionRegistry};

mod jq;

/// JsonFunctions module.
pub struct JsonFunctions;

/// JsonFunctions module registration.
impl FunctionModule for JsonFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(crate::jq::Jq);
    }
}
