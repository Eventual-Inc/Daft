#![feature(let_chains)]

mod function_args;

use proc_macro_error::proc_macro_error;

#[proc_macro_error]
#[proc_macro_derive(FunctionArgs)]
/// Proc macro for deriving `TryFrom<FunctionArgs<T>>` for an argument struct.
///
/// Tests are located at `src/daft-dsl/src/functions/macro_tests.rs`
pub fn derive_function_args(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    function_args::derive_function_args(input)
}
