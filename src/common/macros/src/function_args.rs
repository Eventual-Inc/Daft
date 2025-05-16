use proc_macro2::Ident;
use proc_macro_crate::{crate_name, FoundCrate};
use proc_macro_error::{abort, abort_call_site};
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, FieldsNamed, GenericArgument,
    GenericParam, Generics, PathArguments, Type, TypePath,
};

#[derive(PartialEq, Eq)]
enum FieldType {
    /// `T`
    Required,
    /// `Option<T>`
    Optional,
    /// `Vec<T>`
    Variadic,
}

pub fn derive_function_args(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let daft_dsl = get_crate_name("daft-dsl");

    let input = parse_macro_input!(input as DeriveInput);

    let generic_ident = get_generic_ident(&input.generics);
    let fields = get_fields(&input.data);

    let num_fields = fields.named.len();

    let name = input.ident;
    let field_names = get_field_names(fields);
    let field_types = get_field_types(fields, generic_ident);

    // variadic args cannot be referenced by name
    let named_args = field_names
        .iter()
        .zip(field_types.iter())
        .filter_map(|(name, ty)| match ty {
            FieldType::Required | FieldType::Optional => Some(name),
            FieldType::Variadic => None,
        });

    let field_getters = field_names.iter().zip(field_types.iter()).map(|(name, ty)| {
        match ty {
            FieldType::Required => {
                quote! {
                    unnamed
                        .pop_front()
                        .or_else(|| named.remove(stringify!(#name)))
                        .ok_or_else(|| common_error::DaftError::ValueError(format!("Required argument `{}` not found", stringify!(#name))))?
                }
            },
            FieldType::Optional => {
                quote! {
                    unnamed
                        .pop_front()
                        .or_else(|| named.remove(stringify!(#name)))
                }
            },
            FieldType::Variadic => {
                quote! {
                    unnamed.drain(..).collect()
                }
            },
        }
    });

    let expanded = quote! {
        impl<T> std::convert::TryFrom<#daft_dsl::functions::FunctionArgs<T>> for #name<T> {
            type Error = common_error::DaftError;

            fn try_from(args: #daft_dsl::functions::FunctionArgs<T>) -> common_error::DaftResult<Self> {
                let (unnamed, mut named) = args.into_unnamed_and_named()?;
                let mut unnamed = std::collections::VecDeque::from(unnamed);

                let parsed = Self {
                    #(
                        #field_names: #field_getters,
                    )*
                };

                if !unnamed.is_empty() {
                    return std::result::Result::Err(
                        common_error::DaftError::ValueError(format!("Expected {} arguments, received: {}", #num_fields, #num_fields + unnamed.len()))
                    );
                }

                if !named.is_empty() {
                    return std::result::Result::Err(
                        common_error::DaftError::ValueError(format!("Expected argument names {}, received: {}",
                            [#(stringify!(#named_args)),*]
                                .into_iter()
                                .map(|s| format!("`{}`", s))
                                .collect::<Vec<_>>()
                                .join(", "),
                            named.keys()
                                .map(|s| format!("`{}`", s))
                                .collect::<Vec<_>>()
                                .join(", "),
                        ))
                    );
                }

                std::result::Result::Ok(parsed)
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

fn get_field_names(fields: &FieldsNamed) -> Vec<&Ident> {
    fields
        .named
        .iter()
        .map(|f| f.ident.as_ref().expect("named fields have idents"))
        .collect()
}

fn get_field_types(fields: &FieldsNamed, generic: &Ident) -> Vec<FieldType> {
    fields
        .named
        .iter()
        .map(|f| {
            if let Type::Path(TypePath { qself: None, path }) = &f.ty {
                if path.is_ident(generic) {
                    return FieldType::Required;
                }

                // check if type in the shape `S<T>` where `T` is the struct generic
                if path.segments.len() == 1 {
                    let segment = path.segments.first().unwrap();

                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        if args.args.len() == 1 {
                            let arg = args.args.first().unwrap();

                            if let GenericArgument::Type(Type::Path(TypePath {
                                qself: None,
                                path: inner_path,
                            })) = arg
                                && inner_path.is_ident(generic)
                            {
                                // check for the value of `S` in the above comment
                                match segment.ident.to_string().as_str() {
                                    "Option" => return FieldType::Optional,
                                    "Vec" => return FieldType::Variadic,
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }

            abort!(
                f.span(),
                "field type must be `T`, `Option<T>`, or `Vec<T>`, where `T` is the struct generic"
            )
        })
        .collect()
}

fn get_crate_name(orig_name: &str) -> Ident {
    let crate_name = match crate_name(orig_name) {
        Ok(FoundCrate::Itself) => "crate".to_string(),
        Ok(FoundCrate::Name(name)) => name,
        Err(_) => {
            abort_call_site!("crate must be available in scope: `{}`", orig_name);
        }
    };

    format_ident!("{}", crate_name)
}
