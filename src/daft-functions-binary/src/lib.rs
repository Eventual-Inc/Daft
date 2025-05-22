mod codecs;
mod concat;
mod decode;
mod encode;
mod kernels;
mod length;
mod utils;

use daft_dsl::functions::{FunctionModule, FunctionRegistry};

pub struct BinaryFunctions;

impl FunctionModule for BinaryFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(concat::BinaryConcat);
        parent.add_fn(decode::BinaryDecode);
        parent.add_fn(decode::BinaryTryDecode);
        parent.add_fn(encode::BinaryEncode);
        parent.add_fn(encode::BinaryTryEncode);
        parent.add_fn(length::BinaryLength);
    }
}
