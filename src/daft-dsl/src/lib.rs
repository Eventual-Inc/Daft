mod arithmetic;
pub mod expr;
pub mod functions;
pub mod join;
pub mod optimization;
#[cfg(feature = "python")]
pub mod python;
pub mod python_udf;
pub use common_metrics::operator_metrics;

#[cfg(feature = "python")]
mod visitor;

mod treenode;
pub use common_treenode;
pub use expr::{
    AggExpr, ApproxPercentileParams, Column, Expr, ExprRef, Operator, PlanRef, ResolvedColumn,
    SketchType, Subquery, SubqueryPlan, UnresolvedColumn, WindowExpr, binary_op, bound_col,
    deduplicate_expr_names, estimated_selectivity, exprs_to_schema, has_agg, is_actor_pool_udf,
    is_partition_compatible, is_udf, left_col, lit, null_lit, resolved_col, right_col,
    unresolved_col,
    window::{WindowBoundary, WindowFrame, WindowSpec, window_to_agg_exprs},
};
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<python::PyExpr>()?;

    parent.add_class::<expr::window::WindowFrame>()?;
    parent.add_class::<expr::window::WindowSpec>()?;
    parent.add_class::<expr::window::PyWindowBoundary>()?;

    parent.add_function(wrap_pyfunction!(python::unresolved_col, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::resolved_col, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::bound_col, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::list_, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::date_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::time_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::timestamp_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::duration_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::interval_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::decimal_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::list_lit, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::udf, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::row_wise_udf, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::batch_udf, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::initialize_udfs, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::eq, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::row_number, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::rank, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::dense_rank, parent)?)?;

    Ok(())
}
