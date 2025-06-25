use daft_dsl::functions::{FunctionModule, FunctionRegistry};

mod deserialize;
mod format;
mod serialize;

/// SerdeFunctions module.
pub struct SerdeFunctions;

/// SerdeFunctions module registration.
impl FunctionModule for SerdeFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(crate::deserialize::Deserialize);
        parent.add_fn(crate::deserialize::TryDeserialize);
        parent.add_fn(crate::serialize::Serialize);
    }
}
