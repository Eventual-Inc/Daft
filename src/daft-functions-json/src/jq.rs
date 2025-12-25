use daft_dsl::functions::prelude::*;

/// Executes a JSON filter on a UTF-8 string array.
///
/// # Arguments
///
/// * `arr` - The input UTF-8 array containing JSON strings.
/// * `filter` - The JSON filter string to execute.
///
/// # Returns
///
/// A `DaftResult` containing the resulting UTF-8 array after applying the filter.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Jq;

#[derive(FunctionArgs)]
struct JqArgs<T> {
    input: T,
    filter: String,
}

#[typetag::serde]
impl ScalarUDF for Jq {
    fn name(&self) -> &'static str {
        "jq"
    }

    fn docstring(&self) -> &'static str {
        "Applies a jq filter to a JSON string expression, returning the result as a string."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let JqArgs { input, .. } = args.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(input.dtype == DataType::Utf8, TypeError: "Input must be a string type");
        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let JqArgs { input, filter } = args.try_into()?;
        jaq::execute(&input, &filter)
    }
}

/// Encapsulate all jq functionality, could be pulled out if needs reuse later!
mod jaq {
    use common_error::{DaftError, DaftResult};
    use daft_core::{
        prelude::{AsArrow, DataType, Utf8Array},
        series::Series,
    };
    use jaq_core::{
        Compiler, Ctx, Filter, Native, RcIter, compile, load,
        load::{Arena, File, Loader},
    };
    use jaq_json::Val;
    use serde_json::Value;

    /// Consider returning a typed series based upon a data_type parameter.
    pub fn execute(input: &Series, filter: &str) -> DaftResult<Series> {
        match input.data_type() {
            DataType::Utf8 => {
                let arr = input.utf8()?;
                execute_jaq_filter(arr, filter).map(daft_core::series::IntoSeries::into_series)
            }
            dt => Err(DaftError::TypeError(format!(
                "jq filter not implemented for {dt}"
            ))),
        }
    }

    /// Compiles the jaq filter string to an executable Filter object.
    fn compile_jaq_filter(filter: &str) -> DaftResult<Filter<Native<Val>>> {
        // these are required to create the loader backed by a `&str` basd "File"
        let arena = Arena::default();
        let file = File {
            path: (),
            code: filter,
        };
        // jaq "parsing" is handled by the loader which creates compile-able "modules"
        let loader = Loader::new(jaq_std::defs().chain(jaq_json::defs()));
        let modules = loader
            .load(&arena, file)
            .map_err(|errs| map_load_errs(filter, errs))?;
        // jaq compiles the "modules" into an executable filter
        let compiler = Compiler::default().with_funs(jaq_std::funs().chain(jaq_json::funs()));
        let filter = compiler
            .compile(modules)
            .map_err(|errs| map_compile_errs(filter, errs))?;
        Ok(filter)
    }

    // This is only marked pub(crate) for mod test since mode test was moved outside this module.
    pub(crate) fn execute_jaq_filter(arr: &Utf8Array, filter: &str) -> DaftResult<Utf8Array> {
        // prepare jaq deps for execution
        let compiled_filter = compile_jaq_filter(filter)?;
        let inputs = RcIter::new(core::iter::empty());

        // used for the output array
        let name = arr.name().to_string();
        #[allow(deprecated, reason = "arrow2 migration")]
        let self_arrow = arr.as_arrow2();

        // execute the filter on each input, mapping to some string result
        let values = self_arrow
            .iter()
            .map(|value| {
                value.map_or(Ok(None), |input| {
                    parse_json(input).and_then(|val| {
                        compiled_filter
                            .run((Ctx::new([], &inputs), val))
                            .map(|res| res.map_err(|err| map_compute_err(filter, err)))
                            .collect::<DaftResult<Vec<_>>>()
                            .map(|values| match values.len() {
                                0 => None,
                                1 => Some(values[0].to_string()),
                                _ => Some(Val::Arr(values.into()).to_string()), // need multiple matches to still be a valid JSON string
                            })
                    })
                })
            })
            .collect::<DaftResult<Utf8Array>>()?;

        // be sure to apply the name and validity of the input
        values
            .rename(&name)
            .with_validity(self_arrow.validity().cloned().map(Into::into))
    }

    /// We need serde_json to parse, but then convert to a jaq Val to be evaluated.
    fn parse_json(input: &str) -> DaftResult<Val> {
        let v: Value = serde_json::from_str(input)?;
        let v: Val = v.into();
        Ok(v)
    }

    /// Combine all jaq parsing (load) errors into a list.
    fn map_load_errs(filter: &str, errs: load::Errors<&str, ()>) -> DaftError {
        // had to add the `.collect` to ensure all branches are the same type
        let errs = errs.into_iter().flat_map(|(_, err)| match err {
            load::Error::Io(items) => items.into_iter().map(|(_, e)| e).collect::<Vec<String>>(),
            load::Error::Lex(items) => items
                .into_iter()
                .map(|(_, e)| e.to_string())
                .collect::<Vec<String>>(),
            load::Error::Parse(items) => items
                .into_iter()
                .map(|(_, e)| e.to_string())
                .collect::<Vec<String>>(),
        });
        DaftError::ValueError(format!(
            "Error parsing jq filter ({filter}): {}",
            errs.collect::<Vec<_>>().join(", ")
        ))
    }

    /// Combine all jaq compilation errors into a list.
    fn map_compile_errs(filter: &str, errs: compile::Errors<&str, ()>) -> DaftError {
        DaftError::ComputeError(format!(
            "Error compiling jq filter ({filter}): {}",
            errs.into_iter()
                .flat_map(|(_, errs)| errs.into_iter().map(|(err, _)| err.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    /// Converts an error that would occur during execution.
    fn map_compute_err(filter: &str, err: jaq_core::Error<Val>) -> DaftError {
        DaftError::ComputeError(format!("Error running jq filter ({filter}): {err}"))
    }
}

#[cfg(test)]
#[allow(deprecated, reason = "arrow2 migration")]
mod tests {
    use daft_core::prelude::{AsArrow, Utf8Array};

    use super::*;

    #[test]
    fn test_jaq() -> DaftResult<()> {
        let data = Utf8Array::from_values(
            "data",
            vec![
                r#"{"foo": {"bar": 1}}"#.to_string(),
                r#"{"foo": {"bar": 2}}"#.to_string(),
                r#"{"foo": {"bar": 3}}"#.to_string(),
            ]
            .into_iter(),
        );

        let filter = r".foo.bar";
        let result = jaq::execute_jaq_filter(&data, filter)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.as_arrow2().value(0), "1");
        assert_eq!(result.as_arrow2().value(1), "2");
        assert_eq!(result.as_arrow2().value(2), "3");
        Ok(())
    }
}
