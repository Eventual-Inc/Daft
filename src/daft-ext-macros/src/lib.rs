use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::{format_ident, quote};
use syn::{FnArg, ItemFn, Pat, ReturnType, Type, parse_macro_input};

mod types;

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
    name: Option<String>,
}

impl syn::parse::Parse for DaftFuncBatchArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut return_dtype = None;
        let mut name = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            let _eq: syn::Token![=] = input.parse()?;

            if ident == "return_dtype" {
                return_dtype = Some(input.parse::<syn::Expr>()?);
            } else if ident == "name" {
                let lit: syn::LitStr = input.parse()?;
                name = Some(lit.value());
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

        Ok(DaftFuncBatchArgs { return_dtype, name })
    }
}

fn daft_func_batch_impl(
    func: ItemFn,
    args: DaftFuncBatchArgs,
) -> syn::Result<proc_macro2::TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = format_ident!("{}", snake_to_pascal(&fn_name.to_string()));
    let return_dtype = &args.return_dtype;

    let ffi_name_str = args.name.unwrap_or_else(|| fn_name.to_string());
    let fn_name_str = fn_name.to_string();
    let mut name_bytes = ffi_name_str.clone().into_bytes();
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

/// Generates a [`DaftScalarFunction`] implementation for a row-level function.
///
/// The annotated function receives scalar Rust values and returns a scalar value.
/// The macro generates a PascalCase struct that implements `DaftScalarFunction`,
/// automatically vectorizing the function over Arrow arrays.
///
/// Non-`Option` parameters get automatic null propagation — if any input is null,
/// the output is null. `Option<T>` parameters receive the null as `None`.
///
/// # Supported types
///
/// Inputs: `i8`–`i64`, `u8`–`u64`, `f32`, `f64`, `bool`, `&str`, `&[u8]`,
///         and `Option<T>` of any of these.
///
/// Returns: `i8`–`i64`, `u8`–`u64`, `f32`, `f64`, `bool`, `String`,
///          `Option<T>`, or `DaftResult<T>` / `DaftResult<Option<T>>`.
///
/// # Example
///
/// ```ignore
/// use daft_ext::prelude::*;
///
/// #[daft_func]
/// fn greet(name: &str) -> String {
///     format!("Hello, {}!", name)
/// }
///
/// // Generates struct `Greet` implementing `DaftScalarFunction`.
/// // Register with: session.define_function(Arc::new(Greet));
/// ```
#[proc_macro_attribute]
pub fn daft_func(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let attr_args = if attr.is_empty() {
        DaftFuncArgs::default()
    } else {
        parse_macro_input!(attr as DaftFuncArgs)
    };

    match daft_func_impl(func, attr_args.name) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[derive(Default)]
struct DaftFuncArgs {
    name: Option<String>,
}

impl syn::parse::Parse for DaftFuncArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut name = None;
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            let _eq: syn::Token![=] = input.parse()?;
            if ident == "name" {
                let lit: syn::LitStr = input.parse()?;
                name = Some(lit.value());
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
        Ok(DaftFuncArgs { name })
    }
}

/// Parsed information about a single function parameter.
struct ParamInfo {
    /// Name of the parameter.
    name: syn::Ident,
    /// Whether the parameter is Option<T>.
    is_optional: bool,
    /// Type mapping for the inner type (unwrapped from Option if applicable).
    mapping: types::TypeMapping,
}

/// What the user function returns.
enum ReturnKind {
    /// `T` — always produces a value
    Plain,
    /// `Option<T>` — may produce null
    Nullable,
    /// `DaftResult<T>` — may error
    Fallible,
    /// `DaftResult<Option<T>>` — may error or produce null
    FallibleNullable,
}

fn daft_func_impl(
    func: ItemFn,
    override_name: Option<String>,
) -> syn::Result<proc_macro2::TokenStream> {
    let fn_name = &func.sig.ident;
    let struct_name = format_ident!("{}", snake_to_pascal(&fn_name.to_string()));
    let ffi_name_str = override_name.unwrap_or_else(|| fn_name.to_string());
    let mut name_bytes = ffi_name_str.clone().into_bytes();
    name_bytes.push(0);
    let name_lit = Literal::byte_string(&name_bytes);

    // Parse parameters.
    let mut params = Vec::new();
    for arg in &func.sig.inputs {
        let FnArg::Typed(pat_type) = arg else {
            return Err(syn::Error::new_spanned(arg, "expected typed parameter"));
        };
        let Pat::Ident(pat_ident) = pat_type.pat.as_ref() else {
            return Err(syn::Error::new_spanned(
                &pat_type.pat,
                "expected identifier pattern",
            ));
        };
        let name = pat_ident.ident.clone();
        let ty = pat_type.ty.as_ref();

        let (is_optional, inner_ty) = match types::unwrap_option(ty) {
            Some(inner) => (true, inner),
            None => (false, ty),
        };
        let mapping = types::map_input_type(inner_ty)?;
        params.push(ParamInfo {
            name,
            is_optional,
            mapping,
        });
    }

    // Parse return type.
    let ret_ty = match &func.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "#[daft_func] requires a return type",
            ));
        }
        ReturnType::Type(_, ty) => ty.as_ref(),
    };

    let (return_kind, scalar_ret_ty) = classify_return(ret_ty)?;
    let ret_mapping = types::map_return_type(scalar_ret_ty)?;

    let param_count = params.len();
    let return_data_type = &ret_mapping.data_type;

    // Generate code for downcast + iteration.
    let cg = quote! { ::daft_ext::helpers::_codegen };
    let array_downcasts: Vec<_> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let var = format_ident!("__arr{}", i);
            // The downcast tokens reference `arrays[__idx]` — substitute __idx with i.
            let downcast = &p.mapping.downcast;
            quote! {
                let __idx = #i;
                let #var = #downcast;
            }
        })
        .collect();

    // Generate value extraction + null check + function call per row.
    let value_extractions: Vec<_> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let arr_var = format_ident!("__arr{}", i);
            let val_var = &p.name;
            let value_at = &p.mapping.value_at;
            if p.is_optional {
                // Pass Option<T> to user function
                quote! {
                    let #val_var = if #cg::Array::is_null(#arr_var, __i) {
                        None
                    } else {
                        Some(#arr_var #value_at)
                    };
                }
            } else {
                // Non-optional: extract value, null check done separately
                quote! {
                    let #val_var = #arr_var #value_at;
                }
            }
        })
        .collect();

    // Null propagation: if any non-optional arg is null, output is null.
    let non_optional_null_checks: Vec<_> = params
        .iter()
        .enumerate()
        .filter(|(_, p)| !p.is_optional)
        .map(|(i, _)| {
            let arr_var = format_ident!("__arr{}", i);
            quote! { #cg::Array::is_null(#arr_var, __i) }
        })
        .collect();

    let has_null_check = !non_optional_null_checks.is_empty();
    let null_check = if has_null_check {
        quote! { #(#non_optional_null_checks)||* }
    } else {
        quote! { false }
    };

    let call_args: Vec<_> = params
        .iter()
        .map(|p| {
            let name = &p.name;
            quote! { #name }
        })
        .collect();

    let builder_init = &ret_mapping.builder_init;
    let append_value = &ret_mapping.append_value;
    let append_null = &ret_mapping.append_null;
    let finish = &ret_mapping.finish;

    // Generate the inner call + append based on return kind.
    let call_and_append = match return_kind {
        ReturnKind::Plain => quote! {
            let __val = #fn_name(#(#call_args),*);
            #append_value
        },
        ReturnKind::Nullable => quote! {
            match #fn_name(#(#call_args),*) {
                Some(__val) => { #append_value }
                None => { #append_null }
            }
        },
        ReturnKind::Fallible => quote! {
            let __val = #fn_name(#(#call_args),*)?;
            #append_value
        },
        ReturnKind::FallibleNullable => quote! {
            match #fn_name(#(#call_args),*)? {
                Some(__val) => { #append_value }
                None => { #append_null }
            }
        },
    };

    let loop_body = quote! {
        if #null_check {
            #append_null
        } else {
            #(#value_extractions)*
            #call_and_append
        }
    };

    // Generate type validation for return_field (enables overload resolution).
    let type_validations: Vec<_> = params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let expected_dtype = &p.mapping.data_type;
            let idx = i;
            quote! {
                {
                    let __field = ::daft_ext::prelude::import_field(&args[#idx])?;
                    let __expected = #expected_dtype;
                    if *__field.data_type() != __expected {
                        return Err(::daft_ext::prelude::DaftError::TypeError(
                            format!(
                                "{}: argument {} has type {:?}, expected {:?}",
                                #ffi_name_str, #idx, __field.data_type(), __expected,
                            ),
                        ));
                    }
                }
            }
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
                            #ffi_name_str, #param_count, args.len()
                        ),
                    ));
                }
                // Validate input types for overload resolution.
                #(#type_validations)*

                let name = if args.is_empty() {
                    #ffi_name_str.to_string()
                } else {
                    ::daft_ext::prelude::import_field(&args[0])?
                        .name()
                        .clone()
                };
                let field = ::daft_ext::helpers::new_field(&name, #return_data_type);
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
                            #ffi_name_str, #param_count, args.len()
                        ),
                    ));
                }
                let arrays: Vec<_> = args
                    .into_iter()
                    .map(::daft_ext::prelude::import_array)
                    .collect::<::daft_ext::prelude::DaftResult<_>>()?;

                let __len = if arrays.is_empty() { 0 } else {
                    #cg::Array::len(arrays[0].as_ref())
                };

                #(#array_downcasts)*

                #builder_init

                for __i in 0..__len {
                    #loop_body
                }

                let __output = #finish;
                ::daft_ext::prelude::export_array(__output, #ffi_name_str)
            }
        }
    })
}

fn classify_return(ty: &Type) -> syn::Result<(ReturnKind, &Type)> {
    // DaftResult<Option<T>> or Result<Option<T>, ...>
    if let Some(inner) = types::unwrap_result(ty) {
        if let Some(scalar) = types::unwrap_option(inner) {
            return Ok((ReturnKind::FallibleNullable, scalar));
        }
        return Ok((ReturnKind::Fallible, inner));
    }
    // Option<T>
    if let Some(inner) = types::unwrap_option(ty) {
        return Ok((ReturnKind::Nullable, inner));
    }
    // plain T
    Ok((ReturnKind::Plain, ty))
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
