use common_error::DaftResult;

use crate::series::Series;

pub fn run_python_binary_operator_fn(
    lhs: &Series,
    rhs: &Series,
    operator_fn: &str,
) -> DaftResult<Series> {
    python_binary_op_with_utilfn(lhs, rhs, operator_fn, "map_operator_arrow_semantics")
}

pub fn run_python_binary_bool_operator(
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
    use pyo3::prelude::*;

    use crate::{datatypes::DataType, python::PySeries};

    let lhs = lhs.cast(&DataType::Python)?;
    let rhs = rhs.cast(&DataType::Python)?;

    let (lhs, rhs) = match (lhs.len(), rhs.len()) {
        (a, b) if a == b => (lhs, rhs),
        (a, 1) => (lhs, rhs.broadcast(a)?),
        (1, b) => (lhs.broadcast(b)?, rhs),
        (a, b) => panic!("Cannot apply operation on arrays of different lengths: {a} vs {b}"),
    };

    let left_pylist = PySeries::from(lhs.clone()).to_pylist()?;
    let right_pylist = PySeries::from(rhs).to_pylist()?;

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

pub fn py_membership_op_utilfn(lhs: &Series, rhs: &Series) -> DaftResult<Series> {
    use pyo3::prelude::*;

    use crate::{datatypes::DataType, python::PySeries};

    let lhs_casted = lhs.cast(&DataType::Python)?;
    let rhs_casted = rhs.cast(&DataType::Python)?;

    let left_pylist = PySeries::from(lhs_casted.clone()).to_pylist()?;
    let right_pylist = PySeries::from(rhs_casted).to_pylist()?;

    let result_series: Series = Python::with_gil(|py| -> PyResult<PySeries> {
        let result_pylist = PyModule::import_bound(py, pyo3::intern!(py, "daft.utils"))?
            .getattr(pyo3::intern!(py, "python_list_membership_check"))?
            .call1((left_pylist, right_pylist))?;

        PyModule::import_bound(py, pyo3::intern!(py, "daft.series"))?
            .getattr(pyo3::intern!(py, "Series"))?
            .getattr(pyo3::intern!(py, "from_pylist"))?
            .call1((
                result_pylist,
                lhs_casted.name(),
                pyo3::intern!(py, "disallow"),
            ))?
            .getattr(pyo3::intern!(py, "_series"))?
            .extract()
    })?
    .into();

    Ok(result_series)
}

pub fn py_between_op_utilfn(value: &Series, lower: &Series, upper: &Series) -> DaftResult<Series> {
    use pyo3::prelude::*;

    use crate::{datatypes::DataType, python::PySeries};

    let value_casted = value.cast(&DataType::Python)?;
    let lower_casted = lower.cast(&DataType::Python)?;
    let upper_casted = upper.cast(&DataType::Python)?;

    let (value_casted, lower_casted, upper_casted) =
        match (value_casted.len(), lower_casted.len(), upper_casted.len()) {
            (a, b, c) if a == b && b == c => (value_casted, lower_casted, upper_casted),
            (1, a, b) if a == b => (value_casted.broadcast(a)?, lower_casted, upper_casted),
            (a, 1, b) if a == b => (value_casted, lower_casted.broadcast(a)?, upper_casted),
            (a, b, 1) if a == b => (value_casted, lower_casted, upper_casted.broadcast(a)?),
            (a, 1, 1) => (
                value_casted,
                lower_casted.broadcast(a)?,
                upper_casted.broadcast(a)?,
            ),
            (1, a, 1) => (
                value_casted.broadcast(a)?,
                lower_casted,
                upper_casted.broadcast(a)?,
            ),
            (1, 1, a) => (
                value_casted.broadcast(a)?,
                lower_casted.broadcast(a)?,
                upper_casted,
            ),
            (a, b, c) => {
                panic!("Cannot apply operation on arrays of different lengths: {a} vs {b} vs {c}")
            }
        };

    let value_pylist = PySeries::from(value_casted.clone()).to_pylist()?;
    let lower_pylist = PySeries::from(lower_casted).to_pylist()?;
    let upper_pylist = PySeries::from(upper_casted).to_pylist()?;

    let result_series: Series = Python::with_gil(|py| -> PyResult<PySeries> {
        let result_pylist = PyModule::import_bound(py, pyo3::intern!(py, "daft.utils"))?
            .getattr(pyo3::intern!(py, "python_list_between_check"))?
            .call1((value_pylist, lower_pylist, upper_pylist))?;

        PyModule::import_bound(py, pyo3::intern!(py, "daft.series"))?
            .getattr(pyo3::intern!(py, "Series"))?
            .getattr(pyo3::intern!(py, "from_pylist"))?
            .call1((
                result_pylist,
                value_casted.name(),
                pyo3::intern!(py, "disallow"),
            ))?
            .getattr(pyo3::intern!(py, "_series"))?
            .extract()
    })?
    .into();

    Ok(result_series)
}
