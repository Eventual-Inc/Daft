use pyo3::prelude::*;
mod expr;

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<expr::PyExpr>()?;
    parent.add_wrapped(wrap_pyfunction!(expr::col))?;
    parent.add_wrapped(wrap_pyfunction!(expr::lit))?;

    Ok(())
}
