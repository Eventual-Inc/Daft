#![feature(async_closure)]
pub mod binary;
pub mod coalesce;
pub mod count_matches;
pub mod distance;
pub mod float;
pub mod hash;
pub mod image;
pub mod list;
pub mod minhash;
pub mod numeric;
#[cfg(feature = "python")]
pub mod python;
pub mod temporal;
pub mod to_struct;
pub mod tokenize;
pub mod uri;
pub mod utf8;

use common_error::DaftError;
#[cfg(feature = "python")]
pub use python::register as register_modules;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        Self::new(std::io::ErrorKind::Other, err)
    }
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        Self::External(err.into())
    }
}
