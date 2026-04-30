mod codecs;
mod decode;
mod encode;
mod kernels;
pub use codecs::Codec;
use daft_dsl::functions::{FunctionModule, FunctionRegistry};
pub use decode::{BinaryDecode, BinaryTryDecode};
pub use encode::{BinaryEncode, BinaryTryEncode};

pub struct BinaryFunctions;

impl FunctionModule for BinaryFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(decode::BinaryDecode);
        parent.add_fn(decode::BinaryTryDecode);
        parent.add_fn(encode::BinaryEncode);
        parent.add_fn(encode::BinaryTryEncode);
    }
}
