use crate::utils::supertype::try_get_supertype;
use common_error::DaftResult;

use super::Series;

pub mod abs;
pub mod agg;
pub mod arithmetic;
pub mod between;
pub mod broadcast;
pub mod cast;
pub mod ceil;
pub mod comparison;
pub mod concat;
pub mod downcast;
mod exp;
pub mod filter;
pub mod float;
pub mod floor;
pub mod groups;
pub mod hash;
pub mod if_else;
pub mod image;
pub mod is_in;
pub mod json;
pub mod len;
pub mod list;
pub mod log;
pub mod map;
pub mod minhash;
pub mod not;
pub mod null;
pub mod partitioning;
pub mod repeat;
pub mod round;
pub mod search_sorted;
pub mod shift;
pub mod sign;
pub mod sketch_percentile;
pub mod sort;
pub mod sqrt;
pub mod struct_;
pub mod take;
pub mod time;
mod trigonometry;
pub mod utf8;

pub fn cast_series_to_supertype(series: &[&Series]) -> DaftResult<Vec<Series>> {
    let supertype = series
        .iter()
        .map(|s| s.data_type().clone())
        .try_reduce(|l, r| try_get_supertype(&l, &r))?
        .unwrap();

    series.iter().map(|s| s.cast(&supertype)).collect()
}

#[cfg(feature = "python")]
macro_rules! py_binary_op_utilfn {
    ($lhs:expr, $rhs:expr, $pyoperator:expr, $utilfn:expr) => {{
        use crate::python::PySeries;
        use crate::DataType;
        use pyo3::prelude::*;

        let lhs = $lhs.cast(&DataType::Python)?;
        let rhs = $rhs.cast(&DataType::Python)?;

        let (lhs, rhs) = match (lhs.len(), rhs.len()) {
            (a, b) if a == b => (lhs, rhs),
            (a, 1) => (lhs, rhs.broadcast(a)?),
            (1, b) => (lhs.broadcast(b)?, rhs),
            (a, b) => panic!("Cannot apply operation on arrays of different lengths: {a} vs {b}"),
        };

        let left_pylist = PySeries::from(lhs.clone()).to_pylist()?;
        let right_pylist = PySeries::from(rhs.clone()).to_pylist()?;

        let result_series: Series = Python::with_gil(|py| -> PyResult<PySeries> {
            let py_operator = PyModule::import(py, pyo3::intern!(py, "operator"))?
                .getattr(pyo3::intern!(py, $pyoperator))?;

            let result_pylist = PyModule::import(py, pyo3::intern!(py, "daft.utils"))?
                .getattr(pyo3::intern!(py, $utilfn))?
                .call1((py_operator, left_pylist, right_pylist))?;

            PyModule::import(py, pyo3::intern!(py, "daft.series"))?
                .getattr(pyo3::intern!(py, "Series"))?
                .getattr(pyo3::intern!(py, "from_pylist"))?
                .call1((result_pylist, lhs.name(), pyo3::intern!(py, "disallow")))?
                .getattr(pyo3::intern!(py, "_series"))?
                .extract()
        })?
        .into();

        result_series
    }};
}
#[cfg(feature = "python")]
pub(super) use py_binary_op_utilfn;

#[cfg(feature = "python")]
pub(super) fn py_membership_op_utilfn(lhs: &Series, rhs: &Series) -> DaftResult<Series> {
    use crate::python::PySeries;
    use crate::DataType;
    use pyo3::prelude::*;

    let lhs_casted = lhs.cast(&DataType::Python)?;
    let rhs_casted = rhs.cast(&DataType::Python)?;

    let left_pylist = PySeries::from(lhs_casted.clone()).to_pylist()?;
    let right_pylist = PySeries::from(rhs_casted.clone()).to_pylist()?;

    let result_series: Series = Python::with_gil(|py| -> PyResult<PySeries> {
        let result_pylist = PyModule::import(py, pyo3::intern!(py, "daft.utils"))?
            .getattr(pyo3::intern!(py, "python_list_membership_check"))?
            .call1((left_pylist, right_pylist))?;

        PyModule::import(py, pyo3::intern!(py, "daft.series"))?
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

#[cfg(feature = "python")]
pub(super) fn py_between_op_utilfn(
    value: &Series,
    lower: &Series,
    upper: &Series,
) -> DaftResult<Series> {
    use crate::python::PySeries;
    use crate::DataType;
    use pyo3::prelude::*;

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
    let lower_pylist = PySeries::from(lower_casted.clone()).to_pylist()?;
    let upper_pylist = PySeries::from(upper_casted.clone()).to_pylist()?;

    let result_series: Series = Python::with_gil(|py| -> PyResult<PySeries> {
        let result_pylist = PyModule::import(py, pyo3::intern!(py, "daft.utils"))?
            .getattr(pyo3::intern!(py, "python_list_between_check"))?
            .call1((value_pylist, lower_pylist, upper_pylist))?;

        PyModule::import(py, pyo3::intern!(py, "daft.series"))?
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
