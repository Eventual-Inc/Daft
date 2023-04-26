use crate::{
    array::ops::{DaftCompare, DaftLogical},
    datatypes::{BooleanArray, BooleanType, DataType},
    error::{DaftError, DaftResult},
    series::Series,
    with_match_comparable_daft_types,
};

use super::match_types_on_series;

#[cfg(feature = "python")]
macro_rules! py_compare {
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

            result_series.downcast::<BooleanType>().cloned()
        }
    }
}

macro_rules! impl_compare {
    ($fname:ident, $pycmp:expr) => {
        fn $fname(&self, rhs: &Series) -> Self::Output {
            let (lhs, rhs) = match_types_on_series(self, rhs)?;

            #[cfg(feature = "python")]
            if lhs.data_type() == &DataType::Python {
                return py_compare!(lhs, rhs, $pycmp);
            }

            let lhs = lhs.as_physical()?;
            let rhs = rhs.as_physical()?;

            with_match_comparable_daft_types!(lhs.data_type(), |$T| {
                let lhs = lhs.downcast::<$T>()?;
                let rhs = rhs.downcast::<$T>()?;
                lhs.$fname(rhs)
            })
        }
    };
}

impl DaftCompare<&Series> for Series {
    type Output = DaftResult<BooleanArray>;

    impl_compare!(equal, "==");
    impl_compare!(not_equal, "!=");
    impl_compare!(lt, "<");
    impl_compare!(lte, "<=");
    impl_compare!(gt, ">");
    impl_compare!(gte, ">=");
}

// macro_rules! impl_logical {
//     ($fname:ident, $pyop:expr) => {
//         fn $fname(&self, other: &Series) -> Self::Output {
//             let (lhs, rhs) = match_types_on_series(self, other)?;

//             #[cfg(feature = "python")]
//             if lhs.data_type() == &DataType::Python {
//                 return py_compare!(lhs, rhs, $pyop);
//             }

//             if lhs.data_type() != &DataType::Boolean {
//                 return Err(DaftError::TypeError(format!(
//                     "Can only perform logical operations on boolean supertype, but got left series {} and right series {} with supertype {}",
//                     self.field(),
//                     other.field(),
//                     lhs.data_type(),
//                 )));
//             }
//             self.downcast::<BooleanType>()?
//                 .$fname(rhs.downcast::<BooleanType>()?)
//         }
//     };
// }

impl DaftLogical<&Series> for Series {
    type Output = DaftResult<BooleanArray>;

    fn and(&self, other: &Series) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, other)?;

        #[cfg(feature = "python")]
        if lhs.data_type() == &DataType::Python {
            return py_compare!(lhs, rhs, "&");
        }

        if lhs.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform logical operations on boolean supertype, but got left series {} and right series {} with supertype {}",
                self.field(),
                other.field(),
                lhs.data_type(),
            )));
        }
        self.downcast::<BooleanType>()?
            .and(rhs.downcast::<BooleanType>()?)
    }
    fn or(&self, rhs: &Series) -> Self::Output {
        if self.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                self.data_type(),
                self.name()
            )));
        } else if rhs.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                rhs.data_type(),
                rhs.name()
            )));
        }
        self.downcast::<BooleanType>()?
            .or(rhs.downcast::<BooleanType>()?)
    }

    fn xor(&self, rhs: &Series) -> Self::Output {
        if self.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                self.data_type(),
                self.name()
            )));
        } else if rhs.data_type() != &DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Can only perform Logical Operations on Boolean DataTypes, got {} for Series {}",
                rhs.data_type(),
                rhs.name()
            )));
        }
        self.downcast::<BooleanType>()?
            .xor(rhs.downcast::<BooleanType>()?)
    }
}
