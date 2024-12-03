use daft_core::prelude::CountMode;
use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(list_chunk, crate::list::chunk, [expr: PyExpr, size: usize]);
simple_python_wrapper!(list_unique_count, crate::list::unique_count, [expr: PyExpr]);
simple_python_wrapper!(list_count, crate::list::count, [expr: PyExpr, mode: CountMode]);
simple_python_wrapper!(explode, crate::list::explode, [expr: PyExpr]);
simple_python_wrapper!(list_get, crate::list::get, [expr: PyExpr, idx: PyExpr, default_value: PyExpr]);
simple_python_wrapper!(list_join, crate::list::join, [expr: PyExpr, delim: PyExpr]);
simple_python_wrapper!(list_max, crate::list::max, [expr: PyExpr]);
simple_python_wrapper!(list_mean, crate::list::mean, [expr: PyExpr]);
simple_python_wrapper!(list_min, crate::list::min, [expr: PyExpr]);
simple_python_wrapper!(list_slice, crate::list::slice, [expr: PyExpr, start: PyExpr, end: PyExpr]);
simple_python_wrapper!(list_sum, crate::list::sum, [expr: PyExpr]);
simple_python_wrapper!(list_value_counts, crate::list::value_counts, [expr: PyExpr]);

#[pyfunction]
pub fn list_sort(expr: PyExpr, desc: PyExpr, nulls_first: PyExpr) -> PyResult<PyExpr> {
    Ok(crate::list::sort(expr.into(), Some(desc.into()), Some(nulls_first.into())).into())
}
