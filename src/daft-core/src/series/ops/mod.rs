use crate::utils::supertype::try_get_supertype;
use common_error::DaftResult;

use super::Series;

pub mod abs;
pub mod agg;
pub mod arithmetic;
pub mod broadcast;
pub mod cast;
pub mod comparison;
pub mod concat;
pub mod date;
pub mod downcast;
pub mod filter;
pub mod float;
pub mod groups;
pub mod hash;
pub mod if_else;
pub mod image;
pub mod len;
pub mod list;
pub mod not;
pub mod null;
pub mod search_sorted;
pub mod sort;
pub mod take;
pub mod utf8;

fn match_types_on_series(l: &Series, r: &Series) -> DaftResult<(Series, Series)> {
    let supertype = try_get_supertype(l.data_type(), r.data_type())?;

    let mut lhs = l.clone();

    let mut rhs = r.clone();

    if !lhs.data_type().eq(&supertype) {
        lhs = lhs.cast(&supertype)?;
    }
    if !rhs.data_type().eq(&supertype) {
        rhs = rhs.cast(&supertype)?;
    }

    Ok((lhs, rhs))
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
