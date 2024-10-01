mod expr;

use std::sync::Mutex;

use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{AsArrow, DataType, Utf8Array},
    series::Series,
};
use daft_dsl::{functions::ScalarFunction, ExprRef};
use expr::JsonQuery;
use itertools::Itertools;
use jaq_interpret::{Ctx, Filter, FilterT, ParseCtx, RcIter};
use lazy_static::lazy_static;
use serde_json::Value;

fn setup_parse_ctx() -> ParseCtx {
    // set up the parse context with the core and std libraries https://github.com/01mf02/jaq/tree/main?tab=readme-ov-file#features
    let mut defs = ParseCtx::new(Vec::new());
    defs.insert_natives(jaq_core::core());
    defs.insert_defs(jaq_std::std());
    defs
}

lazy_static! {
    static ref PARSE_CTX: Mutex<ParseCtx> = Mutex::new(setup_parse_ctx());
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

fn json_query_impl(arr: &Utf8Array, query: &str) -> DaftResult<Utf8Array> {
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

pub fn json_query_series(s: &Series, query: &str) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Utf8 => {
            let arr = s.utf8()?;
            json_query_impl(arr, query).map(daft_core::series::IntoSeries::into_series)
        }
        dt => Err(DaftError::TypeError(format!(
            "json query not implemented for {dt}"
        ))),
    }
}

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
#[must_use]
pub fn json_query(input: ExprRef, query: &str) -> ExprRef {
    ScalarFunction::new(
        JsonQuery {
            query: query.to_string(),
        },
        vec![input],
    )
    .into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{prelude::*, pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "json_query")]
pub fn py_json_query(expr: PyExpr, query: &str) -> PyResult<PyExpr> {
    Ok(json_query(expr.into(), query).into())
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(py_json_query, parent)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
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
        let result = json_query_impl(&data, query)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.as_arrow().value(0), "1");
        assert_eq!(result.as_arrow().value(1), "2");
        assert_eq!(result.as_arrow().value(2), "3");
        Ok(())
    }
}
