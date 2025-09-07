mod function_args;

use proc_macro_error::proc_macro_error;

#[proc_macro_error]
#[proc_macro_derive(FunctionArgs, attributes(arg))]
/// Proc macro for deriving `TryFrom<FunctionArgs<T>>` for an argument struct.
///
/// Tests are located at `src/daft-dsl/src/functions/macro_tests.rs`
///
/// ## Example
/// ```
/// #[derive(FunctionArgs)]
/// impl CustomArgs<T> {
///     // expression/series arg
///     a: T,
///
///     // literal arg with type constraint
///     d: usize
///
///     // optional arg
///     #[arg(optional)]
///     b: Option<T>,
///
///     // variadic arg
///     #[arg(variadic)]
///     c: Vec<T>,
///
///     // renaming args
///     #[arg(name = "e2")]
///     e: T
/// }
/// ```
///
/// ## Typing
/// If the type of a field matches the generic type of the struct, the field will be set to an
/// [`ExprRef`] or [`Series`], depending on the context it is used in.
///
/// You may also set a field to any type that implements [`FromLiteral`], and arguments will automatically
/// be parsed into that type.
///
/// ## Attributes
/// Attributes may be specified in the form `#[arg(...)]` on a field. You may specify multiple.
///
/// | Attribute              | Description                                                                                                                                          |
/// |------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
/// | `#[arg(required)]`     | (Default) Required argument.                                                                                                                         |
/// | `#[arg(optional)`      | Optional argument.<br>- Field type must be `Option<...>`.                                                                                            |
/// | `#[arg(variadic)]`     | Variadic argument (zero or more).<br>- Consumes all remaining unnamed arguments<br>- Cannot be assigned via name<br>- Field type must be `Vec<...>`. |
/// | `#[arg(name = "...")]` | Override the name of an argument. By default, field name is used as argument.                                                                        |
pub fn derive_function_args(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    function_args::derive_function_args(input)
}
