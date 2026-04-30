#![allow(deprecated, reason = "arrow2 migration")]

use daft_dsl::functions::{FunctionModule, FunctionRegistry};

mod deserialize;
mod format;
mod serialize;

/// SerdeFunctions module.
pub struct SerdeFunctions;

/// SerdeFunctions module registration.
impl FunctionModule for SerdeFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(self::deserialize::Deserialize);
        parent.add_fn(self::deserialize::TryDeserialize);
        parent.add_fn(self::serialize::Serialize);
    }
}
