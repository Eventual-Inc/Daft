use daft_dsl::python::PyExpr;
use pyo3::{pyfunction, PyResult};

simple_python_wrapper!(dt_date, crate::temporal::dt_date, [expr: PyExpr]);
simple_python_wrapper!(dt_day, crate::temporal::dt_day, [expr: PyExpr]);
simple_python_wrapper!(dt_day_of_week, crate::temporal::dt_day_of_week, [expr: PyExpr]);
simple_python_wrapper!(dt_hour, crate::temporal::dt_hour, [expr: PyExpr]);
simple_python_wrapper!(dt_minute, crate::temporal::dt_minute, [expr: PyExpr]);
simple_python_wrapper!(dt_month, crate::temporal::dt_month, [expr: PyExpr]);
simple_python_wrapper!(dt_second, crate::temporal::dt_second, [expr: PyExpr]);
simple_python_wrapper!(dt_time, crate::temporal::dt_time, [expr: PyExpr]);
simple_python_wrapper!(dt_year, crate::temporal::dt_year, [expr: PyExpr]);

#[pyfunction]
pub fn dt_truncate(expr: PyExpr, interval: &str, relative_to: PyExpr) -> PyResult<PyExpr> {
    Ok(crate::temporal::truncate::dt_truncate(expr.into(), interval, relative_to.into()).into())
}
