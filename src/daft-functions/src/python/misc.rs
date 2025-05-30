use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

#[pyfunction]
#[pyo3(name = "struct")]
pub fn to_struct(inputs: Vec<PyExpr>) -> PyResult<PyExpr> {
    let inputs = inputs.into_iter().map(Into::into).collect();
    Ok(crate::to_struct::to_struct(inputs).into())
}
