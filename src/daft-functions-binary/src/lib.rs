mod codecs;
mod concat;
mod decode;
mod encode;
mod kernels;
mod length;
mod slice;
mod utils;
pub use codecs::Codec;
pub use concat::BinaryConcat;
use daft_dsl::functions::{FunctionModule, FunctionRegistry};
pub use decode::{BinaryDecode, BinaryTryDecode};
pub use encode::{BinaryEncode, BinaryTryEncode};
pub use length::BinaryLength;
pub use slice::BinarySlice;

pub struct BinaryFunctions;

impl FunctionModule for BinaryFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(concat::BinaryConcat);
        parent.add_fn(decode::BinaryDecode);
        parent.add_fn(decode::BinaryTryDecode);
        parent.add_fn(encode::BinaryEncode);
        parent.add_fn(encode::BinaryTryEncode);
        parent.add_fn(length::BinaryLength);
        parent.add_fn(slice::BinarySlice);
    }
}
