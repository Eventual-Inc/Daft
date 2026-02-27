use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

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
    let input = parse_macro_input!(item as DeriveInput);
    let ident = &input.ident;
    let name = pascal_to_snake(&ident.to_string());
    let mut name_bytes = name.into_bytes();
    name_bytes.push(0); // null terminator
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
                // SAFETY: literal is null-terminated and valid UTF-8.
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
