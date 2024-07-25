use pyo3::{pyfunction, PyResult};

use crate::parser::DaftParser;

#[pyfunction]
pub fn sql(sql: &str) -> PyResult<String> {
    let ast = DaftParser::parse(sql).expect("parser error");
    Ok(format!("{:?}", ast))
}
