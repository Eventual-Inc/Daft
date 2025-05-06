mod capitalize;
mod contains;
mod endswith;
mod find;
mod ilike;
mod left;
mod length;
mod length_bytes;
mod like;
mod lower;
mod lpad;
mod lstrip;
mod normalize;

//
mod regexp_extract;
mod regexp_extract_all;
mod regexp_match;

pub(crate) mod pad;

mod rpad;
mod startswith;

pub(crate) mod utils;

pub use capitalize::*;
pub use contains::*;
pub use endswith::*;
pub use find::*;
pub use ilike::*;
pub use left::*;
pub use length::*;
pub use length_bytes::*;
pub use like::*;
pub use lower::*;
pub use lpad::*;
pub use lstrip::*;
pub use normalize::*;
//
pub use regexp_extract::*;
pub use regexp_extract_all::*;
pub use regexp_match::*;
pub use rpad::*;
pub use startswith::*;

pub struct Utf8Functions;

impl daft_dsl::functions::FunctionModule for Utf8Functions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(Capitalize);
        parent.add_fn(Contains);
        parent.add_fn(EndsWith);
        parent.add_fn(Find);
        parent.add_fn(ILike);
        parent.add_fn(Left);
        parent.add_fn(Length);
        parent.add_fn(LengthBytes);
        parent.add_fn(Like);
        parent.add_fn(Lower);
        parent.add_fn(LPad);
        parent.add_fn(LStrip);
        parent.add_fn(Normalize);
        parent.add_fn(RegexpExtract);
        parent.add_fn(RegexpExtractAll);
        parent.add_fn(RegexpMatch);

        parent.add_fn(RPad);
        parent.add_fn(StartsWith);
    }
}
