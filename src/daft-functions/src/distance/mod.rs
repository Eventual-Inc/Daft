pub mod cosine;
pub mod dot;

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn dot_distance(a: PyExpr, b: PyExpr) -> PyResult<PyExpr> {
        Ok(super::dot::dot_distance(a.into(), b.into()).into())
    }

    #[pyfunction]
    pub fn cosine_distance(a: PyExpr, b: PyExpr) -> PyResult<PyExpr> {
        Ok(super::cosine::cosine_distance(a.into(), b.into()).into())
    }
}
