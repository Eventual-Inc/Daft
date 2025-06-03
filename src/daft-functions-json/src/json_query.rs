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
pub struct JsonQuery;

#[derive(FunctionArgs)]
struct JsonQueryArgs<T> {
    input: T,
    query: String,
}

#[typetag::serde]
impl ScalarUDF for JsonQuery {
    fn name(&self) -> &'static str {
        "json_query"
    }

    fn docstring(&self) -> &'static str {
        "Extracts a JSON object from a JSON string using a JSONPath expression."
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let JsonQueryArgs {
            input,
            query: _query,
        } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(input.dtype == DataType::Utf8, TypeError: "Input must be a string type");
        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let JsonQueryArgs { input, query } = inputs.try_into()?;
        jq::query_series(&input, &query)
    }
}

/// Encapsulate all jq functionality, could be pulled out if needs reuse later!
mod jq {

    use std::sync::{LazyLock, Mutex};

    use common_error::{DaftError, DaftResult};
    use daft_core::{
        prelude::{AsArrow, DataType, Utf8Array},
        series::Series,
    };
    use itertools::Itertools;
    use jaq_interpret::{Ctx, Filter, FilterT, ParseCtx, RcIter};
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

    /// Consider returning a typed series after `from_json` is implemented.
    pub fn query_series(input: &Series, query: &str) -> DaftResult<Series> {
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

    fn compile_filter(query: &str) -> DaftResult<Filter> {
        // parse the filter
        let (filter, errs) = jaq_parse::parse(query, jaq_parse::main());
        if !errs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Error parsing json query ({query}): {}",
                errs.iter().map(std::string::ToString::to_string).join(", ")
            )));
        }

        // compile the filter executable
        let mut defs = PARSE_CTX.lock().unwrap();
        let compiled_filter = defs.compile(filter.unwrap());
        if !defs.errs.is_empty() {
            return Err(DaftError::ComputeError(format!(
                "Error compiling json query ({query}): {}",
                defs.errs.iter().map(|(e, _)| e.to_string()).join(", ")
            )));
        }

        Ok(compiled_filter)
    }

    // This is only marked pub(crate) for mod test since it outside this module.
    pub(crate) fn json_query_impl(arr: &Utf8Array, query: &str) -> DaftResult<Utf8Array> {
        let compiled_filter = compile_filter(query)?;
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
                            let res = compiled_filter
                                .run((Ctx::new([], &inputs), json.into()))
                                .map(|result| {
                                    result.map(|v| v.to_string()).map_err(|e| {
                                        DaftError::ComputeError(format!(
                                            "Error running json query ({query}): {e}"
                                        ))
                                    })
                                })
                                .collect::<DaftResult<Vec<_>>>()
                                .map(|values| Some(values.join("\n")));
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
        let result = jq::json_query_impl(&data, query)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.as_arrow().value(0), "1");
        assert_eq!(result.as_arrow().value(1), "2");
        assert_eq!(result.as_arrow().value(2), "3");
        Ok(())
    }
}
