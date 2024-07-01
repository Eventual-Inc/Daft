#![feature(let_chains)]
#![feature(if_let_guard)]

mod arithmetic;
mod expr;
pub mod functions;
mod lit;
pub mod optimization;
#[cfg(feature = "python")]
mod pyobject;
#[cfg(feature = "python")]
pub mod python;
mod treenode;
pub use common_treenode;
pub use expr::binary_op;
pub use expr::col;
pub use expr::{resolve_aggexpr, resolve_aggexprs, resolve_expr, resolve_exprs};
pub use expr::{AggExpr, ApproxPercentileParams, Expr, ExprRef, Operator};
pub use lit::{lit, null_lit, Literal, LiteralValue};
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<python::PyExpr>()?;

    parent.add_wrapped(wrap_pyfunction!(python::col))?;
    parent.add_wrapped(wrap_pyfunction!(python::lit))?;
    parent.add_wrapped(wrap_pyfunction!(python::date_lit))?;
    parent.add_wrapped(wrap_pyfunction!(python::time_lit))?;
    parent.add_wrapped(wrap_pyfunction!(python::timestamp_lit))?;
    parent.add_wrapped(wrap_pyfunction!(python::decimal_lit))?;
    parent.add_wrapped(wrap_pyfunction!(python::series_lit))?;
    parent.add_wrapped(wrap_pyfunction!(python::udf))?;
    parent.add_wrapped(wrap_pyfunction!(python::eq))?;
    parent.add_wrapped(wrap_pyfunction!(python::resolve_expr))?;

    Ok(())
}
