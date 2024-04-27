use std::sync::Mutex;

use crate::datatypes::Utf8Array;

use super::as_arrow::AsArrow;
use common_error::{DaftError, DaftResult};
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
            errs.iter().map(|e| e.to_string()).join(", ")
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

impl Utf8Array {
    pub fn json_query(&self, query: &str) -> DaftResult<Utf8Array> {
        let compiled_filter = compile_filter(query)?;
        let inputs = RcIter::new(core::iter::empty());

        let self_arrow = self.as_arrow();
        let arrow_result = self_arrow
            .iter()
            .map(|opt| {
                opt.map_or(Ok(None), |s| {
                    serde_json::from_str::<Value>(s)
                        .map_err(DaftError::from)
                        .and_then(|json| {
                            compiled_filter
                                .run((Ctx::new([], &inputs), json.into()))
                                .map(|result| {
                                    result.map(|v| v.to_string()).map_err(|e| {
                                        DaftError::ComputeError(format!(
                                            "Error running json query ({query}): {e}"
                                        ))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .map(|values| Some(values.join("\n")))
                        })
                })
            })
            .collect::<Result<arrow2::array::Utf8Array<i64>, _>>()?
            .with_validity(self_arrow.validity().cloned());

        Ok(Utf8Array::from((self.name(), Box::new(arrow_result))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_query() -> DaftResult<()> {
        let data = Utf8Array::from((
            "data",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                r#"{"foo": {"bar": 1}}"#.into(),
                r#"{"foo": {"bar": 2}}"#.into(),
                r#"{"foo": {"bar": 3}}"#.into(),
            ])),
        ));

        let query = r#".foo.bar"#;
        let result = &data.json_query(query)?;
        assert_eq!(result.len(), 3);
        assert_eq!(result.as_arrow().value(0), "1");
        assert_eq!(result.as_arrow().value(1), "2");
        assert_eq!(result.as_arrow().value(2), "3");
        Ok(())
    }
}
