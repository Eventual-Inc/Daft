#![allow(
    deprecated,
    reason = "moving over all scalarUDFs to new pattern. Remove once completed!"
)]
pub mod coalesce;
pub mod distance;
pub mod float;
pub mod hash;
pub mod minhash;
pub mod monotonically_increasing_id;
pub mod numeric;
#[cfg(feature = "python")]
pub mod python;
pub mod to_struct;

use common_error::DaftError;
use daft_dsl::functions::FunctionModule;
use hash::HashFunction;
use minhash::MinHashFunction;
#[cfg(feature = "python")]
pub use python::register as register_modules;
use snafu::Snafu;
use to_struct::ToStructFunction;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        Self::other(err)
    }
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        Self::External(err.into())
    }
}

/// TODO chore: cleanup function implementations using error macros
#[macro_export]
macro_rules! invalid_argument_err {
    ($($arg:tt)*)  => {{
        let msg = format!($($arg)*);
        return Err(common_error::DaftError::TypeError(msg).into());
    }};
}

pub struct HashFunctions;

impl FunctionModule for HashFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(HashFunction);
        parent.add_fn(MinHashFunction);
    }
}

pub struct StructFunctions;

impl FunctionModule for StructFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(ToStructFunction);
    }
}
