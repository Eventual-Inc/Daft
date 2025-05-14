use proc_macro2::Ident;
use proc_macro_crate::{crate_name, FoundCrate};
use proc_macro_error::{abort, abort_call_site, emit_call_site_error};
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, FieldsNamed, GenericParam,
    Generics, Type, TypePath,
};

pub fn derive_function_args(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let daft_dsl = get_daft_dsl_crate_name();

    let generic_ident = get_generic_ident(&input.generics);
    let fields = get_fields(&input.data);
    validate_field_types(fields, generic_ident);

    let name = input.ident;
    let field_names = get_field_names(fields);

    let expanded = quote! {
        impl<T> std::convert::TryFrom<#daft_dsl::functions::FunctionArgs<T>> for #name<T> where T: std::clone::Clone {
            type Error = common_error::DaftError;

            fn try_from(args: #daft_dsl::functions::FunctionArgs<T>) -> common_error::DaftResult<Self> {
                let (unnamed, mut named) = args.to_unnamed_and_named()?;
                let mut unnamed = std::collections::VecDeque::from(unnamed);

                let mut pop_arg = |name| {
                    unnamed
                        .pop_front()
                        .or_else(|| named.remove(name))
                        .ok_or_else(|| common_error::DaftError::ValueError(format!("Required argument `{}` not found", name)))
                        .cloned()
                };

                #(
                    let #field_names = pop_arg(stringify!(#field_names))?;
                )*

                if !unnamed.is_empty() {
                    return std::result::Result::Err(
                        common_error::DaftError::ValueError(format!("Received {} extra unnamed arguments", unnamed.len()))
                    );
                }

                if !named.is_empty() {
                    return std::result::Result::Err(
                        common_error::DaftError::ValueError(format!("Received extra named arguments: {}",
                            named.keys()
                                .map(|s| format!("`{}`", s))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                    );
                }

                std::result::Result::Ok(
                    Self {
                        #(#field_names,)*
                    }
                )
            }
        }
    };

    proc_macro::TokenStream::from(expanded)
}

fn get_generic_ident(generics: &Generics) -> &Ident {
    if generics.params.len() != 1 {
        abort!(generics.span(), "expected one generic")
    }

    let param = &generics.params[0];

    match param {
        GenericParam::Type(type_param) => &type_param.ident,
        _ => abort!(
            param.span(),
            "expected generic to be a simple type, such as <T>"
        ),
    }
}

fn get_fields(data: &Data) -> &FieldsNamed {
    let Data::Struct(data_struct) = data else {
        abort_call_site!("can only derive structs")
    };

    let Fields::Named(fields_named) = &data_struct.fields else {
        abort!(
            data_struct.fields.span(),
            "can only derive structs with named fields"
        )
    };

    fields_named
}

/// make sure all field types are the generic
fn validate_field_types(fields: &FieldsNamed, generic: &Ident) {
    for f in &fields.named {
        if let Type::Path(TypePath { qself: None, path }) = &f.ty
            && path.is_ident(generic)
        {
            // field type is valid
        } else {
            abort!(
                f.ty.span(),
                "field types must match the struct generic type"
            )
        }
    }
}

fn get_field_names(fields: &FieldsNamed) -> Vec<&Ident> {
    fields
        .named
        .iter()
        .map(|f| f.ident.as_ref().expect("named fields have idents"))
        .collect()
}

fn get_daft_dsl_crate_name() -> Ident {
    let crate_name = match crate_name("daft-dsl") {
        Ok(FoundCrate::Itself) => "crate".to_string(),
        Ok(FoundCrate::Name(name)) => name,
        Err(_) => {
            emit_call_site_error!("could not find crate `daft-dsl`");
            "daft_dsl".to_string()
        }
    };

    format_ident!("{}", crate_name)
}
