use daft_core::python::PyDataType;
use daft_dsl::python::PyExpr;
use pyo3::{
    pyfunction,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult,
};

// TODO: Migrate to module once these issues are resolved.
//  - currying in modules, https://github.com/Eventual-Inc/Daft/pull/4431
//  - data_type<->sql isomorphism, <placeholder>
#[pyfunction]
fn from_json(text: PyExpr, dtype: PyDataType) -> PyResult<PyExpr> {
    Ok(crate::from_json::from_json(text.expr, dtype.dtype).into())
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(from_json, parent)?)?;
    Ok(())
}
