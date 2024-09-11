use common_error::DaftResult;

use crate::series::Series;

pub(crate) fn run_python_binary_operator_fn(
    lhs: &Series,
    rhs: &Series,
    operator_fn: &str,
) -> DaftResult<Series> {
    python_binary_op_with_utilfn(lhs, rhs, operator_fn, "map_operator_arrow_semantics")
}

pub(crate) fn run_python_binary_bool_operator(
    lhs: &Series,
    rhs: &Series,
    operator_fn: &str,
) -> DaftResult<Series> {
    python_binary_op_with_utilfn(lhs, rhs, operator_fn, "map_operator_arrow_semantics_bool")
}

fn python_binary_op_with_utilfn(
    lhs: &Series,
    rhs: &Series,
    operator_fn: &str,
    util_fn: &str,
) -> DaftResult<Series> {
    use crate::datatypes::DataType;
    use crate::python::PySeries;
    use pyo3::prelude::*;

    let lhs = lhs.cast(&DataType::Python)?;
    let rhs = rhs.cast(&DataType::Python)?;

    let (lhs, rhs) = match (lhs.len(), rhs.len()) {
        (a, b) if a == b => (lhs, rhs),
        (a, 1) => (lhs, rhs.broadcast(a)?),
        (1, b) => (lhs.broadcast(b)?, rhs),
        (a, b) => panic!("Cannot apply operation on arrays of different lengths: {a} vs {b}"),
    };

    let left_pylist = PySeries::from(lhs.clone()).to_pylist()?;
    let right_pylist = PySeries::from(rhs.clone()).to_pylist()?;

    let result_series: Series = Python::with_gil(|py| -> PyResult<PySeries> {
        let py_operator =
            PyModule::import_bound(py, pyo3::intern!(py, "operator"))?.getattr(operator_fn)?;

        let result_pylist = PyModule::import_bound(py, pyo3::intern!(py, "daft.utils"))?
            .getattr(util_fn)?
            .call1((py_operator, left_pylist, right_pylist))?;

        PyModule::import_bound(py, pyo3::intern!(py, "daft.series"))?
            .getattr(pyo3::intern!(py, "Series"))?
            .getattr(pyo3::intern!(py, "from_pylist"))?
            .call1((result_pylist, lhs.name(), pyo3::intern!(py, "disallow")))?
            .getattr(pyo3::intern!(py, "_series"))?
            .extract()
    })?
    .into();
    Ok(result_series)
}
