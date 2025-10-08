use daft_dsl::functions::FunctionModule;

mod bpe;
mod decode;
mod encode;
mod special_tokens;

pub struct TokenizeFunctions;

impl FunctionModule for TokenizeFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(decode::TokenizeDecodeFunction);
        parent.add_fn(encode::TokenizeEncodeFunction);
    }
}
