mod arithmetic;
mod expr;
mod lit;
#[cfg(feature = "python")]
mod pyobject;
pub mod python;
pub mod functions;
pub mod optimization;
pub use expr::binary_op;
pub use expr::col;
pub use expr::{AggExpr, Expr, Operator};
pub use lit::{lit, null_lit};
#[cfg(feature = "python")]
use pyo3::prelude::*;

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<python::PyExpr>()?;

    parent.add_wrapped(wrap_pyfunction!(python::col))?;
    parent.add_wrapped(wrap_pyfunction!(python::lit))?;
    parent.add_wrapped(wrap_pyfunction!(python::udf))?;
    parent.add_wrapped(wrap_pyfunction!(python::eq))?;

    Ok(())
}