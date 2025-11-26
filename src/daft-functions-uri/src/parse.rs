use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::Serialize;
use url::Url;

#[derive(Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct UrlParse;

#[derive(FunctionArgs)]
pub struct UrlParseArgs<T> {
    pub input: T,
}

#[typetag::serde]
impl ScalarUDF for UrlParse {
    fn name(&self) -> &'static str {
        "url_parse"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let UrlParseArgs { input } = inputs.try_into()?;
        let array = input.utf8()?;
        let result = url_parse(array)?;
        Ok(result.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UrlParseArgs { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;

        if !field.dtype.is_string() {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a string, got {}",
                field.dtype
            )));
        }

        let struct_fields = vec![
            Field::new("scheme", DataType::Utf8),
            Field::new("username", DataType::Utf8),
            Field::new("password", DataType::Utf8),
            Field::new("host", DataType::Utf8),
            Field::new("port", DataType::UInt16),
            Field::new("path", DataType::Utf8),
            Field::new("query", DataType::Utf8),
            Field::new("fragment", DataType::Utf8),
        ];

        Ok(Field::new(field.name, DataType::Struct(struct_fields)))
    }
}

fn url_parse(array: &Utf8Array) -> DaftResult<StructArray> {
    let name = array.name();
    let len = array.len();

    let mut schemes = Vec::with_capacity(len);
    let mut usernames = Vec::with_capacity(len);
    let mut passwords = Vec::with_capacity(len);
    let mut hosts = Vec::with_capacity(len);
    let mut ports = Vec::with_capacity(len);
    let mut paths = Vec::with_capacity(len);
    let mut queries = Vec::with_capacity(len);
    let mut fragments = Vec::with_capacity(len);

    for i in 0..len {
        match array.get(i) {
            None => {
                schemes.push(None);
                usernames.push(None);
                passwords.push(None);
                hosts.push(None);
                ports.push(None);
                paths.push(None);
                queries.push(None);
                fragments.push(None);
            }
            Some(url_str) => match Url::parse(url_str) {
                Ok(url) => {
                    schemes.push(Some(url.scheme().to_string()));
                    usernames.push(if url.username().is_empty() {
                        None
                    } else {
                        Some(url.username().to_string())
                    });
                    passwords.push(url.password().map(|s| s.to_string()));
                    hosts.push(url.host_str().map(|s| s.to_string()));
                    ports.push(url.port());
                    paths.push(Some(url.path().to_string()));
                    queries.push(url.query().map(|s| s.to_string()));
                    fragments.push(url.fragment().map(|s| s.to_string()));
                }
                Err(_) => {
                    schemes.push(None);
                    usernames.push(None);
                    passwords.push(None);
                    hosts.push(None);
                    ports.push(None);
                    paths.push(None);
                    queries.push(None);
                    fragments.push(None);
                }
            },
        }
    }

    let scheme_array = Utf8Array::from_iter("scheme", schemes.into_iter());
    let username_array = Utf8Array::from_iter("username", usernames.into_iter());
    let password_array = Utf8Array::from_iter("password", passwords.into_iter());
    let host_array = Utf8Array::from_iter("host", hosts.into_iter());
    let port_array =
        UInt16Array::from_iter(Field::new("port", DataType::UInt16), ports.into_iter());
    let path_array = Utf8Array::from_iter("path", paths.into_iter());
    let query_array = Utf8Array::from_iter("query", queries.into_iter());
    let fragment_array = Utf8Array::from_iter("fragment", fragments.into_iter());

    let struct_array = StructArray::new(
        Field::new(
            name,
            DataType::Struct(vec![
                Field::new("scheme", DataType::Utf8),
                Field::new("username", DataType::Utf8),
                Field::new("password", DataType::Utf8),
                Field::new("host", DataType::Utf8),
                Field::new("port", DataType::UInt16),
                Field::new("path", DataType::Utf8),
                Field::new("query", DataType::Utf8),
                Field::new("fragment", DataType::Utf8),
            ]),
        ),
        vec![
            scheme_array.into_series(),
            username_array.into_series(),
            password_array.into_series(),
            host_array.into_series(),
            port_array.into_series(),
            path_array.into_series(),
            query_array.into_series(),
            fragment_array.into_series(),
        ],
        None,
    );

    Ok(struct_array)
}
