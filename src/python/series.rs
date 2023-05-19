use std::ops::{Add, Div, Mul, Rem, Sub};

use pyo3::{exceptions::PyValueError, prelude::*, pyclass::CompareOp, types::PyList};

use crate::{
    array::{ops::DaftLogical, pseudo_arrow::PseudoArrowArray, DataArray},
    datatypes::{DataType, Field, PythonType, UInt64Type},
    ffi,
    series::{self, IntoSeries, Series},
    utils::arrow::{cast_array_for_daft_if_needed, cast_array_from_daft_if_needed},
};

use super::datatype::PyDataType;
use crate::array::ops::as_arrow::AsArrow;

#[pyclass]
#[derive(Clone)]
pub struct PySeries {
    pub series: series::Series,
}

#[pymethods]
impl PySeries {
    #[staticmethod]
    pub fn from_arrow(name: &str, pyarrow_array: &PyAny) -> PyResult<Self> {
        let arrow_array = ffi::array_to_rust(pyarrow_array)?;
        let arrow_array = cast_array_for_daft_if_needed(arrow_array.to_boxed());
        let series = series::Series::try_from((name, arrow_array))?;
        Ok(series.into())
    }

    // This ingests a Python list[object] directly into a Rust PythonArray.
    #[staticmethod]
    pub fn from_pylist(name: &str, pylist: &PyAny) -> PyResult<Self> {
        let vec_pyobj: Vec<PyObject> = pylist.extract()?;
        let arrow_array: Box<dyn arrow2::array::Array> =
            Box::new(PseudoArrowArray::<PyObject>::from_pyobj_vec(vec_pyobj));
        let field = Field::new(name, DataType::Python);

        let data_array = DataArray::<PythonType>::new(field.into(), arrow_array)?;
        Ok(data_array.into_series().into())
    }

    // This is for PythonArrays only,
    // to convert the Rust PythonArray to a Python list[object].
    pub fn to_pylist(&self) -> PyResult<PyObject> {
        let pseudo_arrow_array = self.series.python()?.as_arrow();
        let pyobj_vec = pseudo_arrow_array.to_pyobj_vec();
        Python::with_gil(|py| Ok(PyList::new(py, pyobj_vec).into()))
    }

    pub fn to_arrow(&self) -> PyResult<PyObject> {
        let arrow_array = self.series.to_arrow();
        let arrow_array = cast_array_from_daft_if_needed(arrow_array);
        Python::with_gil(|py| {
            let pyarrow = py.import("pyarrow")?;
            ffi::to_py_array(arrow_array, py, pyarrow)
        })
    }

    pub fn __abs__(&self) -> PyResult<Self> {
        Ok(self.series.abs()?.into())
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

    pub fn __and__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.and(&other.series)?.into_series().into())
    }

    pub fn __or__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.or(&other.series)?.into_series().into())
    }

    pub fn __xor__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.xor(&other.series)?.into_series().into())
    }

    pub fn take(&self, idx: &Self) -> PyResult<Self> {
        Ok(self.series.take(&idx.series)?.into())
    }

    pub fn slice(&self, start: i64, end: i64) -> PyResult<Self> {
        if start < 0 {
            return Err(PyValueError::new_err(format!(
                "slice start can not be negative: {start}"
            )));
        }
        if end < 0 {
            return Err(PyValueError::new_err(format!(
                "slice end can not be negative: {start}"
            )));
        }
        if start > end {
            return Err(PyValueError::new_err(format!(
                "slice length can not be negative: start: {start} end: {end}"
            )));
        }
        Ok(self.series.slice(start as usize, end as usize)?.into())
    }

    pub fn filter(&self, mask: &Self) -> PyResult<Self> {
        if mask.series.data_type() != &DataType::Boolean {
            return Err(PyValueError::new_err(format!(
                "We can only filter a Series with Boolean Series, got {}",
                mask.series.data_type()
            )));
        }
        Ok(self.series.filter(mask.series.downcast()?)?.into())
    }

    pub fn sort(&self, descending: bool) -> PyResult<Self> {
        Ok(self.series.sort(descending)?.into())
    }

    pub fn argsort(&self, descending: bool) -> PyResult<Self> {
        Ok(self.series.argsort(descending)?.into())
    }

    pub fn hash(&self, seed: Option<PySeries>) -> PyResult<Self> {
        let seed_series;
        let mut seed_array = None;
        if let Some(s) = seed {
            if s.series.data_type() != &DataType::UInt64 {
                return Err(PyValueError::new_err(format!(
                    "We can only use UInt64 as a seed for hashing, got {}",
                    s.series.data_type()
                )));
            }
            seed_series = s.series;
            seed_array = Some(seed_series.downcast::<UInt64Type>()?);
        }
        Ok(self.series.hash(seed_array)?.into_series().into())
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

    pub fn __invert__(&self) -> PyResult<Self> {
        use std::ops::Not;
        Ok((&self.series).not()?.into())
    }

    pub fn _count(&self) -> PyResult<Self> {
        Ok((self.series).count(None)?.into())
    }

    pub fn _sum(&self) -> PyResult<Self> {
        Ok((self.series).sum(None)?.into())
    }

    pub fn _mean(&self) -> PyResult<Self> {
        Ok((self.series).mean(None)?.into())
    }

    pub fn _min(&self) -> PyResult<Self> {
        Ok((self.series).min(None)?.into())
    }

    pub fn _max(&self) -> PyResult<Self> {
        Ok((self.series).max(None)?.into())
    }

    pub fn _agg_list(&self) -> PyResult<Self> {
        Ok((self.series).agg_list(None)?.into())
    }

    pub fn cast(&self, dtype: PyDataType) -> PyResult<Self> {
        Ok(self.series.cast(&dtype.into())?.into())
    }

    #[staticmethod]
    pub fn concat(series: Vec<Self>) -> PyResult<Self> {
        let series: Vec<_> = series.iter().map(|s| &s.series).collect();
        Ok(Series::concat(series.as_slice())?.into())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.series))
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.series.len())
    }

    pub fn size_bytes(&self) -> PyResult<usize> {
        Ok(self.series.size_bytes()?)
    }

    pub fn name(&self) -> PyResult<String> {
        Ok(self.series.name().to_string())
    }

    pub fn rename(&self, name: &str) -> PyResult<Self> {
        Ok(self.series.rename(name).into())
    }

    pub fn data_type(&self) -> PyResult<PyDataType> {
        Ok(self.series.data_type().clone().into())
    }

    pub fn utf8_endswith(&self, pattern: &Self) -> PyResult<Self> {
        Ok(self.series.utf8_endswith(&pattern.series)?.into())
    }

    pub fn utf8_startswith(&self, pattern: &Self) -> PyResult<Self> {
        Ok(self.series.utf8_startswith(&pattern.series)?.into())
    }

    pub fn utf8_contains(&self, pattern: &Self) -> PyResult<Self> {
        Ok(self.series.utf8_contains(&pattern.series)?.into())
    }

    pub fn utf8_length(&self) -> PyResult<Self> {
        Ok(self.series.utf8_length()?.into())
    }

    pub fn is_nan(&self) -> PyResult<Self> {
        Ok(self.series.is_nan()?.into())
    }

    pub fn dt_day(&self) -> PyResult<Self> {
        Ok(self.series.dt_day()?.into())
    }

    pub fn dt_month(&self) -> PyResult<Self> {
        Ok(self.series.dt_month()?.into())
    }

    pub fn dt_year(&self) -> PyResult<Self> {
        Ok(self.series.dt_year()?.into())
    }

    pub fn dt_day_of_week(&self) -> PyResult<Self> {
        Ok(self.series.dt_day_of_week()?.into())
    }

    pub fn arr_lengths(&self) -> PyResult<Self> {
        Ok(self.series.arr_lengths()?.into_series().into())
    }

    pub fn if_else(&self, other: &Self, predicate: &Self) -> PyResult<Self> {
        Ok(self
            .series
            .if_else(&other.series, &predicate.series)?
            .into())
    }

    pub fn is_null(&self) -> PyResult<Self> {
        Ok(self.series.is_null()?.into())
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
