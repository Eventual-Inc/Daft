use std::ops::{Add, Div, Mul, Rem, Sub};

use pyo3::{prelude::*, pyclass::CompareOp};

use crate::{array::BaseArray, ffi, series};

use super::datatype::PyDataType;

#[pyclass]
pub struct PySeries {
    pub series: series::Series,
}

#[pymethods]
impl PySeries {
    #[staticmethod]
    pub fn from_arrow(name: &str, pyarrow_array: &PyAny) -> PyResult<Self> {
        let arrow_array = ffi::array_to_rust(pyarrow_array)?;
        use arrow2::compute::cast;
        let arrow_array = match arrow_array.data_type() {
            arrow2::datatypes::DataType::Utf8 => {
                cast::utf8_to_large_utf8(arrow_array.as_ref().as_any().downcast_ref().unwrap())
                    .boxed()
            }
            arrow2::datatypes::DataType::Binary => cast::binary_to_large_binary(
                arrow_array.as_ref().as_any().downcast_ref().unwrap(),
                arrow2::datatypes::DataType::LargeBinary,
            )
            .boxed(),
            _ => arrow_array,
        };
        let series = series::Series::try_from((name, arrow_array))?;
        Ok(series.into())
    }

    pub fn to_arrow(&self) -> PyResult<PyObject> {
        let arrow_array = self.series.array().data().to_boxed();
        Python::with_gil(|py| {
            let pyarrow = py.import("pyarrow")?;
            ffi::to_py_array(arrow_array, py, pyarrow)
        })
    }

    pub fn __add__(&self, other: &Self) -> PyResult<Self> {
        Ok((&self.series).add(&other.series)?.into())
    }

    pub fn __sub__(&self, other: &Self) -> PyResult<Self> {
        Ok((&self.series).sub(&other.series)?.into())
    }

    pub fn __mul__(&self, other: &Self) -> PyResult<Self> {
        Ok((&self.series).mul(&other.series)?.into())
    }

    pub fn __truediv__(&self, other: &Self) -> PyResult<Self> {
        Ok((&self.series).div(&other.series)?.into())
    }

    pub fn __mod__(&self, other: &Self) -> PyResult<Self> {
        Ok((&self.series).rem(&other.series)?.into())
    }

    pub fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<Self> {
        use crate::array::ops::DaftCompare;
        match op {
            CompareOp::Lt => Ok((self.series.lt(&other.series)?).into_series().into()),
            CompareOp::Le => Ok((self.series.lte(&other.series)?).into_series().into()),
            CompareOp::Eq => Ok((self.series.equal(&other.series)?).into_series().into()),
            CompareOp::Ne => Ok((self.series.not_equal(&other.series)?).into_series().into()),
            CompareOp::Gt => Ok((self.series.gt(&other.series)?).into_series().into()),
            CompareOp::Ge => Ok((self.series.gte(&other.series)?).into_series().into()),
        }
    }

    pub fn cast(&self, dtype: PyDataType) -> PyResult<Self> {
        Ok(self.series.cast(&dtype.into())?.into())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.series))
    }

    pub fn name(&self) -> PyResult<String> {
        Ok(self.series.name().to_string())
    }

    pub fn data_type(&self) -> PyResult<PyDataType> {
        Ok(self.series.data_type().clone().into())
    }
}

impl From<series::Series> for PySeries {
    fn from(value: series::Series) -> Self {
        PySeries { series: value }
    }
}

impl From<PySeries> for series::Series {
    fn from(item: PySeries) -> Self {
        item.series
    }
}
