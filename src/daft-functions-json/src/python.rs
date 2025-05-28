use daft_core::python::PyDataType;
use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, types::{PyModule, PyModuleMethods}, wrap_pyfunction, Bound, PyResult};

// TODO: Migrate to module once these issues are resolved.
//  - Currying in modules: https://github.com/Eventual-Inc/Daft/pull/4431
//  - DataType<->Text isomorphism: 
#[pyfunction]
fn from_json(text: PyExpr, target: PyDataType) -> PyResult<PyExpr> {
    Ok(text)
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(from_json, parent)?)?;
    Ok(())
}
