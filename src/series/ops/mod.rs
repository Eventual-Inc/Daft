use crate::error::DaftResult;
use crate::utils::supertype::try_get_supertype;

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
pub mod full;
pub mod groups;
pub mod hash;
pub mod if_else;
pub mod len;
pub mod list;
pub mod not;
pub mod null;
pub mod pairwise;
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
macro_rules! py_binary_op {
    ($lhs:expr, $rhs:expr, $pycmp:expr) => {
        {
            use crate::python::PySeries;
            use pyo3::prelude::*;
            use pyo3::types::IntoPyDict;

            let left_pylist = PySeries::from($lhs.clone()).to_pylist()?;
            let right_pylist = PySeries::from($rhs.clone()).to_pylist()?;

            let result_series: Self = Python::with_gil(|py| -> PyResult<PySeries> {
                // Note: In general, we should probably try to keep Python code in Python,
                // to take advantage of the Python toolchain, e.g. mypy guarantees.
                // In this case, it is difficult to call syntactic operators like ==.
                // It is not as simple as calling __xx__, there is a lot of reflection and precedence logic;
                // see https://docs.python.org/3/reference/datamodel.html#object.__lt__ etc for more details.
                let result_pylist = py.eval(format!(
                        "[bool(l {} r) if (l is not None and r is not None) else None for (l, r) in zip(lhs, rhs)]",
                        $pycmp
                    ).as_str(),
                    None,
                    Some([("lhs", left_pylist), ("rhs", right_pylist)].into_py_dict(py)),
                )?;

                PyModule::import(py, pyo3::intern!(py, "daft.series"))?
                    .getattr(pyo3::intern!(py, "Series"))?
                    .getattr(pyo3::intern!(py, "from_pylist"))?
                    .call1((result_pylist, $lhs.name(), pyo3::intern!(py, "disallow")))?
                    .getattr(pyo3::intern!(py, "_series"))?
                    .extract()
            })?.into();

            result_series
        }
    }
}
#[cfg(feature = "python")]
pub(super) use py_binary_op;
