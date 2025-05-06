mod capitalize;
mod contains;
mod endswith;
mod extract;
mod extract_all;
mod find;
mod ilike;
mod left;
mod length;
mod startswith;
pub(crate) mod utils;

pub use capitalize::*;
pub use contains::*;
pub use endswith::*;
pub use extract::*;
pub use extract_all::*;
pub use find::*;
pub use ilike::*;
pub use left::*;
pub use length::*;
pub use startswith::*;

pub struct Utf8Functions;

impl daft_dsl::functions::FunctionModule for Utf8Functions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(Capitalize);
        parent.add_fn(Contains);
        parent.add_fn(EndsWith);
        parent.add_fn(RegexpExtract);
        parent.add_fn(RegexpExtractAll);
        parent.add_fn(Find);
        parent.add_fn(ILike);
        parent.add_fn(Left);
        parent.add_fn(Length);

        parent.add_fn(StartsWith);
    }
}
