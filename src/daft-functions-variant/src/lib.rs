use daft_dsl::functions::{FunctionModule, FunctionRegistry};

mod parse_json;
mod variant_get;
mod variant_to_json;

pub struct VariantFunctions;

impl FunctionModule for VariantFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(crate::parse_json::ParseJson);
        parent.add_fn(crate::variant_to_json::VariantToJson);
        parent.add_fn(crate::variant_get::VariantGet);
    }
}
