#![allow(
    deprecated,
    reason = "moving over all scalarUDFs to new pattern. Remove once completed!"
)]
pub mod coalesce;
pub mod concat_ws;
pub mod crypto;
pub mod distance;
pub mod float;
pub mod hash;
pub mod length;
pub mod minhash;
pub mod monotonically_increasing_id;
pub mod numeric;
#[cfg(feature = "python")]
pub mod python;
pub mod random;
pub mod simhash;
pub mod similarity;
pub mod slice;
pub mod to_struct;
pub mod uuid;
pub mod vector_utils;

use common_error::DaftError;
use crypto::{Crc32Function, Md5Function, Sha1Function, Sha2Function, XxHash64Function};
use daft_dsl::functions::{FunctionModule, FunctionRegistry};
use hash::HashFunction;
use length::Length;
use minhash::MinHashFunction;
#[cfg(feature = "python")]
pub use python::register as register_modules;
use simhash::SimHashFunction;
use snafu::Snafu;
use to_struct::ToStructFunction;
use uuid::{
    ExtractDayUuid7, ExtractHourUuid7, ExtractMinuteUuid7, ExtractMonthUuid7, Uuid, UuidV7,
};

use crate::{concat_ws::ConcatWs, slice::Slice};

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

pub struct MiscFunctions;

impl FunctionModule for MiscFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(ConcatWs);
        parent.add_fn(HashFunction);
        parent.add_fn(MinHashFunction);
        parent.add_fn(SimHashFunction);
        parent.add_fn(Length);
        parent.add_fn(ToStructFunction);
        parent.add_fn(Slice);
        // Crypto/hash functions
        parent.add_fn(Md5Function);
        parent.add_fn(Sha1Function);
        parent.add_fn(Sha2Function);
        parent.add_fn(XxHash64Function);
        parent.add_fn(Crc32Function);
>>>>>>> 8beb69497 (feat: implement hash/crypto functions (md5, sha1, sha2, xxhash64, crc32))
    }
}
=======
        parent.add_fn(Uuid);
        parent.add_fn(UuidV7);
        parent.add_fn(ExtractMinuteUuid7);
        parent.add_fn(ExtractHourUuid7);
        parent.add_fn(ExtractDayUuid7);
        parent.add_fn(ExtractMonthUuid7);
        // Crypto/hash functions
        parent.add_fn(Md5Function);
        parent.add_fn(Sha1Function);
        parent.add_fn(Sha2Function);
        parent.add_fn(XxHash64Function);
        parent.add_fn(Crc32Function);
    }
}
=======
        // Crypto/hash functions
        parent.add_fn(Md5Function);
        parent.add_fn(Sha1Function);
        parent.add_fn(Sha2Function);
        parent.add_fn(XxHash64Function);
        parent.add_fn(Crc32Function);
>>>>>>> 8beb69497 (feat: implement hash/crypto functions (md5, sha1, sha2, xxhash64, crc32))
    }
}
