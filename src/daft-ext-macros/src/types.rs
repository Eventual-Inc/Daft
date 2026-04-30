use proc_macro2::TokenStream;
use quote::quote;
use syn::{Type, spanned::Spanned};

pub struct TypeMapping {
    pub data_type: TokenStream,
    pub downcast: TokenStream,
    pub value_at: TokenStream,
}

pub struct ReturnTypeMapping {
    pub data_type: TokenStream,
    pub builder_init: TokenStream,
    pub append_value: TokenStream,
    pub append_null: TokenStream,
    pub finish: TokenStream,
}

fn cg() -> TokenStream {
    quote! { ::daft_ext::helpers::_codegen }
}

fn primitive_input(arrow_type: TokenStream, data_type: TokenStream) -> TypeMapping {
    let cg = cg();
    TypeMapping {
        data_type,
        downcast: quote! {
            arrays[__idx].as_any()
                .downcast_ref::<#cg::PrimitiveArray<#arrow_type>>()
                .ok_or_else(|| ::daft_ext::prelude::DaftError::TypeError(
                    format!("expected PrimitiveArray<{}>", stringify!(#arrow_type))
                ))?
        },
        value_at: quote! { .value(__i) },
    }
}

fn primitive_return(arrow_type: TokenStream, data_type: TokenStream) -> ReturnTypeMapping {
    let cg = cg();
    ReturnTypeMapping {
        data_type,
        builder_init: quote! {
            let mut __builder = #cg::PrimitiveBuilder::<#arrow_type>::with_capacity(__len);
        },
        append_value: quote! { __builder.append_value(__val); },
        append_null: quote! { __builder.append_null(); },
        finish: quote! { #cg::Arc::new(__builder.finish()) as #cg::ArrayRef },
    }
}

pub fn map_input_type(ty: &Type) -> syn::Result<TypeMapping> {
    let cg = cg();
    let type_str = type_to_string(ty);

    match type_str.as_str() {
        "i8" => Ok(primitive_input(
            quote! { #cg::Int8Type },
            quote! { #cg::DataType::Int8 },
        )),
        "i16" => Ok(primitive_input(
            quote! { #cg::Int16Type },
            quote! { #cg::DataType::Int16 },
        )),
        "i32" => Ok(primitive_input(
            quote! { #cg::Int32Type },
            quote! { #cg::DataType::Int32 },
        )),
        "i64" => Ok(primitive_input(
            quote! { #cg::Int64Type },
            quote! { #cg::DataType::Int64 },
        )),
        "u8" => Ok(primitive_input(
            quote! { #cg::UInt8Type },
            quote! { #cg::DataType::UInt8 },
        )),
        "u16" => Ok(primitive_input(
            quote! { #cg::UInt16Type },
            quote! { #cg::DataType::UInt16 },
        )),
        "u32" => Ok(primitive_input(
            quote! { #cg::UInt32Type },
            quote! { #cg::DataType::UInt32 },
        )),
        "u64" => Ok(primitive_input(
            quote! { #cg::UInt64Type },
            quote! { #cg::DataType::UInt64 },
        )),
        "f32" => Ok(primitive_input(
            quote! { #cg::Float32Type },
            quote! { #cg::DataType::Float32 },
        )),
        "f64" => Ok(primitive_input(
            quote! { #cg::Float64Type },
            quote! { #cg::DataType::Float64 },
        )),
        "bool" => Ok(TypeMapping {
            data_type: quote! { #cg::DataType::Boolean },
            downcast: quote! {
                arrays[__idx].as_any()
                    .downcast_ref::<#cg::BooleanArray>()
                    .ok_or_else(|| ::daft_ext::prelude::DaftError::TypeError(
                        "expected BooleanArray".into()
                    ))?
            },
            value_at: quote! { .value(__i) },
        }),
        "& str" => Ok(TypeMapping {
            data_type: quote! { #cg::DataType::Utf8 },
            downcast: quote! {
                arrays[__idx].as_any()
                    .downcast_ref::<#cg::LargeStringArray>()
                    .ok_or_else(|| ::daft_ext::prelude::DaftError::TypeError(
                        "expected LargeStringArray".into()
                    ))?
            },
            value_at: quote! { .value(__i) },
        }),
        "& [u8]" => Ok(TypeMapping {
            data_type: quote! { #cg::DataType::Binary },
            downcast: quote! {
                arrays[__idx].as_any()
                    .downcast_ref::<#cg::LargeBinaryArray>()
                    .ok_or_else(|| ::daft_ext::prelude::DaftError::TypeError(
                        "expected LargeBinaryArray".into()
                    ))?
            },
            value_at: quote! { .value(__i) },
        }),
        _ => {
            // Try Vec<T> → LargeListArray
            if let Some(inner) = unwrap_vec(ty) {
                return map_vec_input(inner);
            }
            // Try [T; N] → FixedSizeListArray
            if let Type::Array(arr) = ty {
                return map_fixed_array_input(&arr.elem, &arr.len);
            }
            Err(syn::Error::new(
                ty.span(),
                format!("unsupported input type `{type_str}` for #[daft_func]"),
            ))
        }
    }
}

fn map_vec_input(inner_ty: &Type) -> syn::Result<TypeMapping> {
    let cg = cg();
    let inner = map_input_type(inner_ty)?;
    let inner_data_type = &inner.data_type;
    Ok(TypeMapping {
        data_type: quote! {
            #cg::DataType::LargeList(
                #cg::Arc::new(#cg::ArrowField::new("item", #inner_data_type, true))
            )
        },
        downcast: quote! {
            arrays[__idx].as_any()
                .downcast_ref::<#cg::LargeListArray>()
                .ok_or_else(|| ::daft_ext::prelude::DaftError::TypeError(
                    "expected LargeListArray".into()
                ))?
        },
        value_at: quote! { {
            let __list_arr = #cg::Array::slice(&#cg::LargeListArray::value(__arr, __i), 0, #cg::Array::len(&#cg::LargeListArray::value(__arr, __i)));
            let __inner = __list_arr
                .as_any()
                .downcast_ref::<#cg::PrimitiveArray<_>>()
                .expect("expected primitive inner array");
            __inner.values().iter().copied().collect::<Vec<_>>()
        } },
    })
}

fn map_fixed_array_input(elem: &Type, len_expr: &syn::Expr) -> syn::Result<TypeMapping> {
    let cg = cg();
    let inner = map_input_type(elem)?;
    let inner_data_type = &inner.data_type;
    Ok(TypeMapping {
        data_type: quote! {
            #cg::DataType::FixedSizeList(
                #cg::Arc::new(#cg::ArrowField::new("item", #inner_data_type, true)),
                #len_expr
            )
        },
        downcast: quote! {
            arrays[__idx].as_any()
                .downcast_ref::<#cg::FixedSizeListArray>()
                .ok_or_else(|| ::daft_ext::prelude::DaftError::TypeError(
                    "expected FixedSizeListArray".into()
                ))?
        },
        value_at: quote! { {
            let __fsl_values = #cg::FixedSizeListArray::values(__arr);
            let __inner = __fsl_values
                .as_any()
                .downcast_ref::<#cg::PrimitiveArray<_>>()
                .expect("expected primitive inner array");
            let __offset = __i * (#len_expr as usize);
            ::std::array::from_fn(|__j| __inner.value(__offset + __j))
        } },
    })
}

pub fn map_return_type(ty: &Type) -> syn::Result<ReturnTypeMapping> {
    let cg = cg();
    let type_str = type_to_string(ty);

    match type_str.as_str() {
        "i8" => Ok(primitive_return(
            quote! { #cg::Int8Type },
            quote! { #cg::DataType::Int8 },
        )),
        "i16" => Ok(primitive_return(
            quote! { #cg::Int16Type },
            quote! { #cg::DataType::Int16 },
        )),
        "i32" => Ok(primitive_return(
            quote! { #cg::Int32Type },
            quote! { #cg::DataType::Int32 },
        )),
        "i64" => Ok(primitive_return(
            quote! { #cg::Int64Type },
            quote! { #cg::DataType::Int64 },
        )),
        "u8" => Ok(primitive_return(
            quote! { #cg::UInt8Type },
            quote! { #cg::DataType::UInt8 },
        )),
        "u16" => Ok(primitive_return(
            quote! { #cg::UInt16Type },
            quote! { #cg::DataType::UInt16 },
        )),
        "u32" => Ok(primitive_return(
            quote! { #cg::UInt32Type },
            quote! { #cg::DataType::UInt32 },
        )),
        "u64" => Ok(primitive_return(
            quote! { #cg::UInt64Type },
            quote! { #cg::DataType::UInt64 },
        )),
        "f32" => Ok(primitive_return(
            quote! { #cg::Float32Type },
            quote! { #cg::DataType::Float32 },
        )),
        "f64" => Ok(primitive_return(
            quote! { #cg::Float64Type },
            quote! { #cg::DataType::Float64 },
        )),
        "bool" => Ok(ReturnTypeMapping {
            data_type: quote! { #cg::DataType::Boolean },
            builder_init: quote! { let mut __builder = #cg::BooleanBuilder::with_capacity(__len); },
            append_value: quote! { __builder.append_value(__val); },
            append_null: quote! { __builder.append_null(); },
            finish: quote! { #cg::Arc::new(__builder.finish()) as #cg::ArrayRef },
        }),
        "String" => Ok(ReturnTypeMapping {
            data_type: quote! { #cg::DataType::Utf8 },
            builder_init: quote! { let mut __builder = #cg::StringBuilder::with_capacity(__len, __len * 32); },
            append_value: quote! { __builder.append_value(__val); },
            append_null: quote! { __builder.append_null(); },
            finish: quote! { #cg::Arc::new(__builder.finish()) as #cg::ArrayRef },
        }),
        _ => {
            // Try Vec<T> → LargeListArray output
            if let Some(inner) = unwrap_vec(ty) {
                return map_vec_return(inner);
            }
            // Try [T; N] → FixedSizeListArray output
            if let Type::Array(arr) = ty {
                return map_fixed_array_return(&arr.elem, &arr.len);
            }
            Err(syn::Error::new(
                ty.span(),
                format!("unsupported return type `{type_str}` for #[daft_func]"),
            ))
        }
    }
}

fn map_vec_return(inner_ty: &Type) -> syn::Result<ReturnTypeMapping> {
    let cg = cg();
    let inner_ret = map_return_type(inner_ty)?;
    let inner_data_type = &inner_ret.data_type;
    Ok(ReturnTypeMapping {
        data_type: quote! {
            #cg::DataType::LargeList(
                #cg::Arc::new(#cg::ArrowField::new("item", #inner_data_type, true))
            )
        },
        builder_init: quote! {
            let mut __builder = #cg::LargeListBuilder::new(
                #cg::PrimitiveBuilder::<_>::new()
            );
        },
        append_value: quote! {
            for __item in __val.iter() {
                __builder.values().append_value(*__item);
            }
            __builder.append(true);
        },
        append_null: quote! { __builder.append_null(); },
        finish: quote! { #cg::Arc::new(__builder.finish()) as #cg::ArrayRef },
    })
}

fn map_fixed_array_return(elem: &Type, len_expr: &syn::Expr) -> syn::Result<ReturnTypeMapping> {
    let cg = cg();
    let inner_ret = map_return_type(elem)?;
    let inner_data_type = &inner_ret.data_type;
    Ok(ReturnTypeMapping {
        data_type: quote! {
            #cg::DataType::FixedSizeList(
                #cg::Arc::new(#cg::ArrowField::new("item", #inner_data_type, true)),
                #len_expr
            )
        },
        builder_init: quote! {
            let mut __builder = #cg::FixedSizeListBuilder::new(
                #cg::PrimitiveBuilder::<_>::new(),
                #len_expr,
            );
        },
        append_value: quote! {
            for __item in __val.iter() {
                __builder.values().append_value(*__item);
            }
            __builder.append(true);
        },
        append_null: quote! { __builder.append_null(); },
        finish: quote! { #cg::Arc::new(__builder.finish()) as #cg::ArrayRef },
    })
}

pub fn unwrap_option(ty: &Type) -> Option<&Type> {
    unwrap_generic(ty, "Option")
}

pub fn unwrap_result(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        let seg = type_path.path.segments.last()?;
        if (seg.ident == "DaftResult" || seg.ident == "Result")
            && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
            && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
        {
            return Some(inner);
        }
    }
    None
}

fn unwrap_vec(ty: &Type) -> Option<&Type> {
    unwrap_generic(ty, "Vec")
}

fn unwrap_generic<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    if let Type::Path(type_path) = ty {
        let seg = type_path.path.segments.last()?;
        if seg.ident == name
            && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
            && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
        {
            return Some(inner);
        }
    }
    None
}

fn type_to_string(ty: &Type) -> String {
    quote!(#ty).to_string()
}
