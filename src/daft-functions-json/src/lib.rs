use daft_dsl::functions::{FunctionModule, FunctionRegistry};

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
pub use python::register_modules;

mod from_json;
mod json_query;

/// JsonFunctions module.
pub struct JsonFunctions;

/// JsonFunctions module registration.
impl FunctionModule for JsonFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(crate::json_query::JsonQuery);
    }
}
