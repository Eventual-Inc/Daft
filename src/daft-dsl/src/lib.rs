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
    binary_op, col, count_actor_pool_udfs, has_agg, is_actor_pool_udf, is_partition_compatible,
    AggExpr, ApproxPercentileParams, Expr, ExprRef, Operator, OuterReferenceColumn, SketchType,
    Subquery, SubqueryPlan,
};
pub use lit::{lit, literal_value, literals_to_series, null_lit, Literal, LiteralValue};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use resolve_expr::{check_column_name_validity, ExprResolver};

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<python::PyExpr>()?;

    parent.add_function(wrap_pyfunction!(python::col, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::date_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::time_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::timestamp_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::duration_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::interval_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::decimal_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::series_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::udf, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::initialize_udfs, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::get_udf_names, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::eq, parent)?)?;
    parent.add_function(wrap_pyfunction!(
        python::check_column_name_validity,
        parent
    )?)?;

    Ok(())
}
