use proc_macro2::{Ident, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use proc_macro_error::{abort, abort_call_site, emit_error};
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, FieldsNamed, GenericArgument,
    GenericParam, Generics, LitStr, PathArguments, Type, TypePath,
};

enum ArgCardinality {
    /// `T`
    Required,
    /// `Option<T>`
    Optional,
    /// `Vec<T>`
    Variadic,
}

enum ArgType {
    Generic,
    #[allow(unused)]
    Concrete(Type),
}

struct ParsedAttribute {
    name_override: Option<String>,
    cardinality: ArgCardinality,
}

impl Default for ParsedAttribute {
    fn default() -> Self {
        Self {
            name_override: None,
            cardinality: ArgCardinality::Required,
        }
    }
}

pub fn derive_function_args(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let daft_dsl = get_crate_name("daft-dsl");
    let daft_core = get_crate_name("daft-core");

    let input = parse_macro_input!(input as DeriveInput);

    let generic_ident = get_generic_ident(&input.generics);
    let fields = get_fields(&input.data);

    let name = input.ident;
    let attrs = get_attrs(fields);
    let field_names = get_field_names(fields);
    let arg_names = get_arg_names(&field_names, &attrs);
    let arg_types = get_arg_types(fields, &attrs, generic_ident);
    let arg_cards = attrs.iter().map(|a| &a.cardinality).collect::<Vec<_>>();

    let expr_to_lit = quote! {
        |expr: #daft_dsl::ExprRef, name: &str| {
            if let #daft_dsl::Expr::Literal(val) = expr.as_ref() {
                std::result::Result::Ok(val.clone())
            } else {
                std::result::Result::Err(
                    common_error::DaftError::ValueError(format!("Expected argument `{}` to be a literal, received: {}", name, expr))
                )
            }
        }
    };

    let series_to_lit = quote! {
        |series: #daft_core::series::Series, _name: &str| #daft_dsl::LiteralValue::try_from_single_value_series(&series)
    };

    let expr_impl = derive_for_type(
        quote!(#daft_dsl::ExprRef),
        expr_to_lit,
        &name,
        &field_names,
        &arg_names,
        &arg_types,
        &arg_cards,
        generic_ident.is_some(),
    );
    let series_impl = derive_for_type(
        quote!(#daft_core::series::Series),
        series_to_lit,
        &name,
        &field_names,
        &arg_names,
        &arg_types,
        &arg_cards,
        generic_ident.is_some(),
    );

    let expanded = quote! {
        #expr_impl
        #series_impl
    };

    expanded.into()
}

#[allow(clippy::too_many_arguments)]
fn derive_for_type(
    ty: TokenStream,
    to_lit_impl: TokenStream,
    name: &Ident,
    field_names: &[&Ident],
    arg_names: &[String],
    arg_types: &[ArgType],
    arg_cards: &[&ArgCardinality],
    has_generic: bool,
) -> TokenStream {
    let daft_dsl = get_crate_name("daft-dsl");

    let num_fields = field_names.len();

    let arg_getters = arg_names
        .iter()
        .zip(arg_types)
        .zip(arg_cards)
        .map(|((n, t), c)| {
            match (c, t) {
                (ArgCardinality::Required, ArgType::Generic) => quote! {
                    unnamed
                        .pop_front()
                        .or_else(|| named.remove(#n))
                        .ok_or_else(|| common_error::DaftError::ValueError(format!("Required argument `{}` not found", #n)))?
                },
                (ArgCardinality::Required, ArgType::Concrete(_)) => quote! {
                    #daft_dsl::FromLiteral::try_from_literal(
                        &to_lit(
                            unnamed
                                .pop_front()
                                .or_else(|| named.remove(#n))
                                .ok_or_else(|| common_error::DaftError::ValueError(format!("Required argument `{}` not found", #n)))?,
                            #n
                        )?
                    )?
                },
                (ArgCardinality::Optional, ArgType::Generic) => quote! {
                    unnamed
                        .pop_front()
                        .or_else(|| named.remove(#n))
                },
                (ArgCardinality::Optional, ArgType::Concrete(_)) => quote! {
                    unnamed
                        .pop_front()
                        .or_else(|| named.remove(#n))
                        .map(|val| #daft_dsl::FromLiteral::try_from_literal(&to_lit(val, #n)?))
                        .transpose()?
                        .flatten()

                },
                (ArgCardinality::Variadic, ArgType::Generic) => quote! {
                    unnamed.drain(..).collect()
                },
                (ArgCardinality::Variadic, ArgType::Concrete(_)) => quote! {
                    unnamed
                        .drain(..)
                        .map(|val| #daft_dsl::FromLiteral::try_from_literal(&to_lit(val, #n)?))
                        .collect::<common_error::DaftResult<_>>()?
                },
            }
        });

    // variadic args cannot be referenced by name
    let named_args = arg_names
        .iter()
        .zip(arg_cards)
        .filter_map(|(n, c)| match c {
            ArgCardinality::Required | ArgCardinality::Optional => Some(format!("`{n}`")),
            ArgCardinality::Variadic => None,
        })
        .collect::<Vec<_>>()
        .join(", ");

    let impl_type = if has_generic {
        quote!(#name<#ty>)
    } else {
        quote!(#name)
    };

    quote! {
        impl std::convert::TryFrom<#daft_dsl::functions::FunctionArgs<#ty>> for #impl_type {
            type Error = common_error::DaftError;

            fn try_from(args: #daft_dsl::functions::FunctionArgs<#ty>) -> common_error::DaftResult<Self> {
                let (unnamed, mut named) = args.into_unnamed_and_named()?;
                let mut unnamed = std::collections::VecDeque::from(unnamed);

                let to_lit = #to_lit_impl;

                let parsed = Self {
                    #(
                        #field_names: #arg_getters,
                    )*
                };

                if !unnamed.is_empty() {
                    return std::result::Result::Err(
                        common_error::DaftError::ValueError(format!("Expected {} arguments, received: {}", #num_fields, #num_fields + unnamed.len()))
                    );
                }

                if !named.is_empty() {
                    return std::result::Result::Err(
                        common_error::DaftError::ValueError(format!("Expected argument names [{}], received: {}",
                            #named_args,
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
    }
}

fn get_generic_ident(generics: &Generics) -> Option<&Ident> {
    match generics.params.len() {
        0 => None,
        1 => {
            let param = &generics.params[0];

            match param {
                GenericParam::Type(type_param) => Some(&type_param.ident),
                _ => abort!(
                    param.span(),
                    "expected generic to be a simple type, such as <T>"
                ),
            }
        }
        _ => abort!(generics.span(), "expected zero or one generics"),
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

fn get_attrs(fields: &FieldsNamed) -> Vec<ParsedAttribute> {
    fields
        .named
        .iter()
        .map(|f| {
            let mut parsed = ParsedAttribute::default();

            for a in &f.attrs {
                if a.path().is_ident("arg") {
                    let result = a.parse_nested_meta(|meta| {
                        let Some(path_ident) = meta.path.get_ident() else {
                            return Err(meta.error("unsupported attribute syntax"));
                        };

                        let ensure_no_args = |name| {
                            if meta.input.is_empty() {
                                Ok(())
                            } else {
                                Err(meta
                                    .input
                                    .error(format!("attribute `{name}` cannot have arguments")))
                            }
                        };

                        match path_ident.to_string().as_str() {
                            "name" => {
                                let name = meta.value()?.parse::<LitStr>()?.value();
                                parsed.name_override = Some(name);
                            }
                            "required" => {
                                ensure_no_args("required")?;

                                parsed.cardinality = ArgCardinality::Required;
                            }
                            "optional" => {
                                ensure_no_args("optional")?;

                                parsed.cardinality = ArgCardinality::Optional;
                            }
                            "variadic" => {
                                ensure_no_args("variadic")?;

                                parsed.cardinality = ArgCardinality::Variadic;
                            }
                            _ => return Err(meta.error("unsupported attribute syntax")),
                        }

                        Ok(())
                    });

                    if let Err(e) = result {
                        emit_error!(a.span(), "error parsing attribute: {}", e)
                    }
                }
            }

            parsed
        })
        .collect()
}

fn get_field_names(fields: &FieldsNamed) -> Vec<&Ident> {
    fields
        .named
        .iter()
        .map(|f| f.ident.as_ref().expect("named fields have idents"))
        .collect()
}

fn get_arg_names(field_names: &[&Ident], attrs: &[ParsedAttribute]) -> Vec<String> {
    field_names
        .iter()
        .zip(attrs)
        .map(|(n, a)| a.name_override.clone().unwrap_or(n.to_string()))
        .collect()
}

fn get_arg_types(
    fields: &FieldsNamed,
    attrs: &[ParsedAttribute],
    generic: Option<&Ident>,
) -> Vec<ArgType> {
    fields
        .named
        .iter()
        .zip(attrs)
        .map(|(f, a)| {
            let ty = match a.cardinality {
                ArgCardinality::Required => &f.ty,
                ArgCardinality::Optional => get_option_type_inner(&f.ty),
                ArgCardinality::Variadic => get_vec_type_inner(&f.ty),
            };

            if let Type::Path(TypePath { qself: None, path }) = ty
                && generic.is_some_and(|g| path.is_ident(g))
            {
                ArgType::Generic
            } else {
                ArgType::Concrete(ty.clone())
            }
        })
        .collect()
}

fn get_option_type_inner(ty: &Type) -> &Type {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        if path.segments.len() == 1 {
            let segment = path.segments.first().unwrap();

            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if args.args.len() == 1 {
                        let arg = args.args.first().unwrap();

                        if let GenericArgument::Type(inner_type) = arg {
                            return inner_type;
                        }
                    }
                }
            }
        }
    }

    abort!(
        ty.span(),
        "If `#[arg(optional)]` specified, field type must be in the form `Option<T>`."
    )
}

fn get_vec_type_inner(ty: &Type) -> &Type {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        if path.segments.len() == 1 {
            let segment = path.segments.first().unwrap();

            if segment.ident == "Vec" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if args.args.len() == 1 {
                        let arg = args.args.first().unwrap();

                        if let GenericArgument::Type(inner_type) = arg {
                            return inner_type;
                        }
                    }
                }
            }
        }
    }

    abort!(
        ty.span(),
        "If `#[arg(variadic)]` specified, field type must be in the form `Vec<T>`."
    )
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
