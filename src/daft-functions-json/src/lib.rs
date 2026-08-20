use daft_dsl::functions::{FunctionModule, FunctionRegistry};

mod jq;
mod json_array_length;
mod json_object_keys;
mod json_tuple;

/// JsonFunctions module.
pub struct JsonFunctions;

/// JsonFunctions module registration.
impl FunctionModule for JsonFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(crate::jq::Jq);
        parent.add_fn(crate::json_array_length::JsonArrayLength);
        parent.add_fn(crate::json_object_keys::JsonObjectKeys);
        parent.add_fn(crate::json_tuple::JsonTuple);
    }
}
