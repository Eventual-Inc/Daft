use daft_dsl::functions::prelude::*;

/// Executes a JSON query on a UTF-8 string array.
///
/// # Arguments
///
/// * `arr` - The input UTF-8 array containing JSON strings.
/// * `query` - The JSON query string to execute.
///
/// # Returns
///
/// A `DaftResult` containing the resulting UTF-8 array after applying the query.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Jq;

#[derive(FunctionArgs)]
struct JqArgs<T> {
    input: T,
    query: String,
}

#[typetag::serde]
impl ScalarUDF for Jq {
    fn name(&self) -> &'static str {
        "jq"
    }

    fn docstring(&self) -> &'static str {
        "Applies a jq query to a JSON string expression, returning the result as a string."
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let JqArgs {
            input,
            query: _query,
        } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(input.dtype == DataType::Utf8, TypeError: "Input must be a string type");
        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let JqArgs { input, query } = inputs.try_into()?;
        jaq::execute(&input, &query)
    }
}

/// Encapsulate all jq functionality, could be pulled out if needs reuse later!
mod jaq {

    use std::sync::{LazyLock, Mutex};

    use common_error::{DaftError, DaftResult};
    use daft_core::{
        prelude::{AsArrow, DataType, Utf8Array},
        series::Series,
    };
    use itertools::Itertools;
    use jaq_interpret::{Ctx, Filter, FilterT, ParseCtx, RcIter, Val};
    use serde_json::Value;

    /// The jaq context with one-time initialization.
    static PARSE_CTX: LazyLock<Mutex<ParseCtx>> = LazyLock::new(|| Mutex::new(create_parse_ctx()));

    /// Create the context with jaq_core and jaq_std, see: https://github.com/01mf02/jaq/tree/main?tab=readme-ov-file#features.
    fn create_parse_ctx() -> ParseCtx {
        let mut defs = ParseCtx::new(Vec::new());
        defs.insert_natives(jaq_core::core());
        defs.insert_defs(jaq_std::std());
        defs
    }

    /// Consider returning a typed series based upon a data_type parameter.
    pub fn execute(input: &Series, query: &str) -> DaftResult<Series> {
        match input.data_type() {
            DataType::Utf8 => {
                let arr = input.utf8()?;
                json_query_impl(arr, query).map(daft_core::series::IntoSeries::into_series)
            }
            dt => Err(DaftError::TypeError(format!(
                "json query not implemented for {dt}"
            ))),
        }
    }

    fn compile_query(query: &str) -> DaftResult<Filter> {
        // parse the query
        let (parsed_query, errs) = jaq_parse::parse(query, jaq_parse::main());
        if !errs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Error parsing json query ({query}): {}",
                errs.iter().map(std::string::ToString::to_string).join(", ")
            )));
        }

        // compile the query
        let mut defs = PARSE_CTX.lock().unwrap();
        let compiled_query = defs.compile(parsed_query.unwrap());
        if !defs.errs.is_empty() {
            return Err(DaftError::ComputeError(format!(
                "Error compiling json query ({query}): {}",
                defs.errs.iter().map(|(e, _)| e.to_string()).join(", ")
            )));
        }

        Ok(compiled_query)
    }

    // This is only marked pub(crate) for mod test since it outside this module.
    pub(crate) fn json_query_impl(arr: &Utf8Array, query: &str) -> DaftResult<Utf8Array> {
        let compiled_query = compile_query(query)?;
        let inputs = RcIter::new(core::iter::empty());

        let self_arrow = arr.as_arrow();
        let name = arr.name().to_string();

        let values = self_arrow
            .iter()
            .map(|opt| {
                opt.map_or(Ok(None), |s| {
                    serde_json::from_str::<Value>(s)
                        .map_err(DaftError::from)
                        .and_then(|json| {
                            let res = compiled_query
                                .run((Ctx::new([], &inputs), json.into()))
                                .map(|result| {
                                    result.map_err(|e| {
                                        DaftError::ComputeError(format!(
                                            "Error running json query ({query}): {e}"
                                        ))
                                    })
                                })
                                .collect::<DaftResult<Vec<_>>>()
                                .map(|values| {
                                    match values.len() {
                                        0 => None,
                                        1 => Some(values[0].to_string()),
                                        _ => Some(Val::arr(values).to_string()), // need multiple matches to still be a valid JSON string
                                    }
                                });
                            res
                        })
                })
            })
            .collect::<DaftResult<Utf8Array>>()?;

        values
            .rename(&name)
            .with_validity(self_arrow.validity().cloned())
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::{AsArrow, Utf8Array};

    use super::*;

    #[test]
    fn test_json_query() -> DaftResult<()> {
        let data = Utf8Array::from_values(
            "data",
            vec![
                r#"{"foo": {"bar": 1}}"#.to_string(),
                r#"{"foo": {"bar": 2}}"#.to_string(),
                r#"{"foo": {"bar": 3}}"#.to_string(),
            ]
            .into_iter(),
        );

        let query = r".foo.bar";
        let result = jaq::json_query_impl(&data, query)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.as_arrow().value(0), "1");
        assert_eq!(result.as_arrow().value(1), "2");
        assert_eq!(result.as_arrow().value(2), "3");
        Ok(())
    }
}
