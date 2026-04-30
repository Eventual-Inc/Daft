use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::{format_ident, quote};
use syn::{ItemFn, parse_macro_input};

/// Generates the `daft_module_magic` entry point for a Daft extension module.
///
/// Place this attribute on a struct that implements `DaftExtension`. The macro
/// generates the `#[no_mangle] pub extern "C" fn daft_module_magic()` symbol
/// that Daft's module loader resolves via `dlopen`.
///
/// # Example
///
/// ```ignore
/// use daft_ext::prelude::*;
///
/// #[daft_extension]
/// struct MyExtension;
///
/// impl DaftExtension for MyExtension {
///     fn install(session: &mut dyn DaftSession) {
///         // register scalar functions here
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn daft_extension(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::DeriveInput);
    let ident = &input.ident;
    let name = pascal_to_snake(&ident.to_string());
    let mut name_bytes = name.into_bytes();
    name_bytes.push(0);
    let name_lit = Literal::byte_string(&name_bytes);

    let output = quote! {
        #input

        #[unsafe(no_mangle)]
        pub extern "C" fn daft_module_magic() -> ::daft_ext::abi::FFI_Module {
            unsafe extern "C" fn __daft_init(
                session: *mut ::daft_ext::abi::FFI_SessionContext,
            ) -> ::std::ffi::c_int {
                let session = unsafe { &mut *session };
                let mut ctx = ::daft_ext::prelude::SessionContext::new(session);
                <#ident as ::daft_ext::prelude::DaftExtension>::install(&mut ctx);
                0
            }

            ::daft_ext::abi::FFI_Module {
                daft_abi_version: ::daft_ext::abi::DAFT_ABI_VERSION,
                name: unsafe {
                    ::std::ffi::CStr::from_bytes_with_nul_unchecked(#name_lit)
                }.as_ptr(),
                init: __daft_init,
                free_string: ::daft_ext::prelude::free_string,
            }
        }
    };

    output.into()
}

/// Generates a [`DaftScalarFunction`] implementation for a batch-level function.
///
/// The annotated function receives arrow-rs `ArrayRef` arguments and returns
/// `DaftResult<ArrayRef>`. The macro generates a PascalCase struct that
/// implements `DaftScalarFunction`, handling all FFI conversion automatically.
///
/// # Attributes
///
/// - `return_dtype = <expr>` — the arrow `DataType` of the output column (required)
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use daft_ext::prelude::*;
/// use arrow_array::{ArrayRef, Float64Array, cast::AsArray, types::Float64Type};
/// use arrow_schema::DataType;
///
/// #[daft_func_batch(return_dtype = DataType::Float64)]
/// fn add_one(input: ArrayRef) -> DaftResult<ArrayRef> {
///     let arr = input.as_primitive::<Float64Type>();
///     let result: ArrayRef = Arc::new(
///         arr.iter()
///             .map(|v| v.map(|x| x + 1.0))
///             .collect::<Float64Array>(),
///     );
///     Ok(result)
/// }
///
/// // Generates struct `AddOne` implementing `DaftScalarFunction`.
/// // Register with: session.define_function(Arc::new(AddOne));
/// ```
#[proc_macro_attribute]
pub fn daft_func_batch(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let attr_args = parse_macro_input!(attr as DaftFuncBatchArgs);

    match daft_func_batch_impl(func, attr_args) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

struct DaftFuncBatchArgs {
    return_dtype: syn::Expr,
}

impl syn::parse::Parse for DaftFuncBatchArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut return_dtype = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            let _eq: syn::Token![=] = input.parse()?;

            if ident == "return_dtype" {
                return_dtype = Some(input.parse::<syn::Expr>()?);
            } else {
                return Err(syn::Error::new(
                    ident.span(),
                    format!("unknown attribute `{ident}`"),
                ));
            }

            if !input.is_empty() {
                let _comma: syn::Token![,] = input.parse()?;
            }
        }

        let return_dtype = return_dtype.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "missing `return_dtype` attribute",
            )
        })?;

        Ok(DaftFuncBatchArgs { return_dtype })
    }
}

fn daft_func_batch_impl(
    func: ItemFn,
    args: DaftFuncBatchArgs,
) -> syn::Result<proc_macro2::TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = format_ident!("{}", snake_to_pascal(&fn_name.to_string()));
    let return_dtype = &args.return_dtype;

    let fn_name_str = fn_name.to_string();
    let mut name_bytes = fn_name_str.clone().into_bytes();
    name_bytes.push(0);
    let name_lit = Literal::byte_string(&name_bytes);

    let param_count = func.sig.inputs.len();

    // Generate parameter bindings: let __arg0 = arrays[0].clone(); ...
    // and the call arguments: __arg0, __arg1, ...
    let param_bindings: Vec<_> = (0..param_count)
        .map(|i| {
            let var = format_ident!("__arg{}", i);
            quote! { let #var = arrays[#i].clone(); }
        })
        .collect();

    let call_args: Vec<_> = (0..param_count)
        .map(|i| {
            let var = format_ident!("__arg{}", i);
            quote! { #var }
        })
        .collect();

    Ok(quote! {
        #func

        pub struct #struct_name;

        impl ::daft_ext::prelude::DaftScalarFunction for #struct_name {
            fn name(&self) -> &::std::ffi::CStr {
                unsafe { ::std::ffi::CStr::from_bytes_with_nul_unchecked(#name_lit) }
            }

            fn return_field(
                &self,
                args: &[::daft_ext::abi::ArrowSchema],
            ) -> ::daft_ext::prelude::DaftResult<::daft_ext::abi::ArrowSchema> {
                if args.len() != #param_count {
                    return Err(::daft_ext::prelude::DaftError::TypeError(
                        format!(
                            "{}: expected {} argument(s), got {}",
                            #fn_name_str, #param_count, args.len()
                        ),
                    ));
                }
                let name = if args.is_empty() {
                    #fn_name_str.to_string()
                } else {
                    ::daft_ext::prelude::import_field(&args[0])?
                        .name()
                        .clone()
                };
                let field = ::daft_ext::helpers::new_field(
                    &name,
                    #return_dtype,
                );
                ::daft_ext::prelude::export_field(&field)
            }

            fn call(
                &self,
                args: Vec<::daft_ext::abi::ArrowData>,
            ) -> ::daft_ext::prelude::DaftResult<::daft_ext::abi::ArrowData> {
                if args.len() != #param_count {
                    return Err(::daft_ext::prelude::DaftError::TypeError(
                        format!(
                            "{}: expected {} argument(s) in call, got {}",
                            #fn_name_str, #param_count, args.len()
                        ),
                    ));
                }
                let arrays: Vec<_> = args
                    .into_iter()
                    .map(::daft_ext::prelude::import_array)
                    .collect::<::daft_ext::prelude::DaftResult<_>>()?;

                #(#param_bindings)*
                let result = #fn_name(#(#call_args),*)?;

                ::daft_ext::prelude::export_array(result, #fn_name_str)
            }
        }
    })
}

/// Convert a PascalCase identifier to snake_case.
fn pascal_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_lowercase().next().unwrap());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Convert a snake_case identifier to PascalCase.
fn snake_to_pascal(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(c) => {
                    let mut out = c.to_uppercase().to_string();
                    out.extend(chars);
                    out
                }
                None => String::new(),
            }
        })
        .collect()
}
