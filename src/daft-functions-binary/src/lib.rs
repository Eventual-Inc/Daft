mod codecs;
mod concat;
mod decode;
mod kernels;
mod utils;

use daft_dsl::functions::{FunctionModule, FunctionRegistry};

pub struct BinaryFunctions;

impl FunctionModule for BinaryFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(concat::BinaryConcat);
        parent.add_fn(decode::BinaryDecode);
        parent.add_fn(decode::BinaryTryDecode);
    }
}
