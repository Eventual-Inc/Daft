use daft_dsl::{functions::ScalarFunction, ExprRef};
use daft_io::IOConfig;
pub use decode::TokenizeDecodeFunction;
pub use encode::TokenizeEncodeFunction;

mod bpe;
mod decode;
mod encode;
mod special_tokens;

pub fn tokenize_encode(
    data: ExprRef,
    tokens_path: &str,
    io_config: Option<IOConfig>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
    use_special_tokens: bool,
) -> ExprRef {
    ScalarFunction::new(
        TokenizeEncodeFunction {
            tokens_path: tokens_path.to_string(),
            io_config: io_config.map(std::convert::Into::into),
            pattern: pattern.map(str::to_string),
            special_tokens: special_tokens.map(str::to_string),
            use_special_tokens,
        },
        vec![data],
    )
    .into()
}

pub fn tokenize_decode(
    data: ExprRef,
    tokens_path: &str,
    io_config: Option<IOConfig>,
    pattern: Option<&str>,
    special_tokens: Option<&str>,
) -> ExprRef {
    ScalarFunction::new(
        TokenizeDecodeFunction {
            tokens_path: tokens_path.to_string(),
            io_config: io_config.map(std::convert::Into::into),
            pattern: pattern.map(str::to_string),
            special_tokens: special_tokens.map(str::to_string),
        },
        vec![data],
    )
    .into()
}
