#![feature(let_chains)]
#![feature(if_let_guard)]

mod arithmetic;
mod expr;
pub mod functions;
pub mod join;
mod lit;
pub mod optimization;
#[cfg(feature = "python")]
mod pyobj_serde;
#[cfg(feature = "python")]
pub mod python;
mod resolve_expr;
mod treenode;
pub use common_treenode;
pub use expr::{
    binary_op, col, has_agg, has_stateful_udf, is_partition_compatible, AggExpr,
    ApproxPercentileParams, Expr, ExprRef, Operator, OuterReferenceColumn, SketchType, Subquery,
    SubqueryPlan,
};
pub use lit::{lit, literal_value, literals_to_series, null_lit, Literal, LiteralValue};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use resolve_expr::{check_column_name_validity, ExprResolver};

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<python::PyExpr>()?;

    parent.add_function(wrap_pyfunction_bound!(python::col, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::date_lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::time_lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::timestamp_lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::duration_lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::interval_lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::decimal_lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::series_lit, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::stateless_udf, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::stateful_udf, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        python::extract_partial_stateful_udf_py,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(python::bind_stateful_udfs, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::eq, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        python::check_column_name_validity,
        parent
    )?)?;

    Ok(())
}
