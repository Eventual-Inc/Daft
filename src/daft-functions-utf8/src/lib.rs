mod capitalize;
mod contains;
mod endswith;

pub use capitalize::capitalize;
use capitalize::Capitalize;
pub use contains::contains;
use contains::Contains;
pub use endswith::endswith;
use endswith::EndsWith;

pub struct Utf8Functions;

impl daft_dsl::functions::FunctionModule for Utf8Functions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(Capitalize);
        parent.add_fn(Contains);
        parent.add_fn(EndsWith);
    }
}
