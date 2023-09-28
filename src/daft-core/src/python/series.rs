use std::ops::{Add, Div, Mul, Rem, Sub};

use pyo3::{exceptions::PyValueError, prelude::*, pyclass::CompareOp, types::PyList};

use crate::{
    array::{ops::DaftLogical, pseudo_arrow::PseudoArrowArray, DataArray},
    count_mode::CountMode,
    datatypes::{DataType, Field, ImageFormat, ImageMode, PythonType},
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
    pub fn from_pylist(name: &str, pylist: &PyAny, pyobj: &str) -> PyResult<Self> {
        let vec_pyobj: Vec<PyObject> = pylist.extract()?;
        let py = pylist.py();
        let dtype = match pyobj {
            "force" => DataType::Python,
            "allow" => infer_daft_dtype_for_sequence(&vec_pyobj, py, name)?.unwrap_or(DataType::Python),
            "disallow" => panic!("Cannot create a Series from a pylist and being strict about only using Arrow types by setting pyobj=disallow"),
            _ => panic!("Unsupported pyobj behavior when creating Series from pylist: {}", pyobj)
        };
        let arrow_array: Box<dyn arrow2::array::Array> =
            Box::new(PseudoArrowArray::<PyObject>::from_pyobj_vec(vec_pyobj));
        let field = Field::new(name, DataType::Python);

        let data_array = DataArray::<PythonType>::new(field.into(), arrow_array)?;
        let series = data_array.cast(&dtype)?;
        Ok(series.into())
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
                "slice end can not be negative: {end}"
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
            seed_array = Some(seed_series.u64()?);
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

    pub fn _count(&self, mode: CountMode) -> PyResult<Self> {
        Ok((self.series).count(None, mode)?.into())
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

    pub fn utf8_split(&self, pattern: &Self) -> PyResult<Self> {
        Ok(self.series.utf8_split(&pattern.series)?.into())
    }

    pub fn utf8_length(&self) -> PyResult<Self> {
        Ok(self.series.utf8_length()?.into())
    }

    pub fn is_nan(&self) -> PyResult<Self> {
        Ok(self.series.is_nan()?.into())
    }

    pub fn dt_date(&self) -> PyResult<Self> {
        Ok(self.series.dt_date()?.into())
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

    pub fn list_lengths(&self) -> PyResult<Self> {
        Ok(self.series.list_lengths()?.into_series().into())
    }

    pub fn image_decode(&self) -> PyResult<Self> {
        Ok(self.series.image_decode()?.into())
    }

    pub fn image_encode(&self, image_format: ImageFormat) -> PyResult<Self> {
        Ok(self.series.image_encode(image_format)?.into())
    }

    pub fn image_resize(&self, w: i64, h: i64) -> PyResult<Self> {
        if w < 0 {
            return Err(PyValueError::new_err(format!(
                "width can not be negative: {w}"
            )));
        }
        if h < 0 {
            return Err(PyValueError::new_err(format!(
                "height can not be negative: {h}"
            )));
        }

        Ok(self.series.image_resize(w as u32, h as u32)?.into())
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

fn infer_daft_dtype_for_sequence(
    vec_pyobj: &[PyObject],
    py: pyo3::Python,
    _name: &str,
) -> PyResult<Option<DataType>> {
    let py_pil_image_type = py
        .import(pyo3::intern!(py, "PIL.Image"))
        .and_then(|m| m.getattr(pyo3::intern!(py, "Image")));
    let np_ndarray_type = py
        .import(pyo3::intern!(py, "numpy"))
        .and_then(|m| m.getattr(pyo3::intern!(py, "ndarray")));
    let np_generic_type = py
        .import(pyo3::intern!(py, "numpy"))
        .and_then(|m| m.getattr(pyo3::intern!(py, "generic")));
    let from_numpy_dtype = {
        py.import(pyo3::intern!(py, "daft.datatype"))?
            .getattr(pyo3::intern!(py, "DataType"))?
            .getattr(pyo3::intern!(py, "from_numpy_dtype"))?
    };
    let mut dtype: Option<DataType> = None;
    for obj in vec_pyobj.iter() {
        let obj = obj.as_ref(py);
        if let Ok(pil_image_type) = py_pil_image_type && obj.is_instance(pil_image_type)? {
            let mode_str = obj
                .getattr(pyo3::intern!(py, "mode"))?
                .extract::<String>()?;
            let mode = ImageMode::from_pil_mode_str(&mode_str)?;
            match &dtype {
                Some(DataType::Image(Some(existing_mode))) => {
                    if *existing_mode != mode {
                        // Mixed-mode case, set mode to None.
                        dtype = Some(DataType::Image(None));
                    }
                }
                None => {
                    // Set to (currently) uniform mode image dtype.
                    dtype = Some(DataType::Image(Some(mode)));
                }
                // No-op, since dtype is already for mixed-mode images.
                Some(DataType::Image(None)) => {}
                _ => {
                    // Images mixed with non-images; short-circuit since union dtypes are not (yet) supported.
                    dtype = None;
                    break;
                }
            }
        } else if let Ok(np_ndarray_type) = np_ndarray_type && let Ok(np_generic_type) = np_generic_type && (obj.is_instance(np_ndarray_type)? || obj.is_instance(np_generic_type)?) {
            let np_dtype = obj.getattr(pyo3::intern!(py, "dtype"))?;
            let inferred_inner_dtype = from_numpy_dtype.call1((np_dtype,)).map(|dt| dt.getattr(pyo3::intern!(py, "_dtype")).unwrap().extract::<PyDataType>().unwrap().dtype);
            let shape: Vec<u64> = obj.getattr(pyo3::intern!(py, "shape"))?.extract()?;
            let inferred_dtype = match inferred_inner_dtype {
                Ok(inferred_inner_dtype) if shape.len() == 1 => Some(DataType::List(Box::new(inferred_inner_dtype))),
                Ok(inferred_inner_dtype) if shape.len() > 1 => Some(DataType::Tensor(Box::new(inferred_inner_dtype))),
                _ => None,
            };
            match (&dtype, &inferred_dtype) {
                // Tensors with mixed inner dtypes is not supported, short-circuit.
                (Some(existing_dtype), Some(inferred_dtype)) if existing_dtype != inferred_dtype => {
                    // TODO(Clark): Do some basic type promotion here, e.g. (u32, u64) --> u64.
                    dtype = None;
                    break;
                }
                // Existing and inferred dtypes must be the same here, so this is a no-op.
                (Some(_), Some(_)) => {}
                // No existing dtype and inferred is non-None, so set cached dtype to inferred.
                (None, Some(inferred_dtype)) => {
                    dtype = Some(inferred_dtype.clone());
                }
                // Inferred inner dtype isn't representable with Arrow, so we'll need to use a plain Python representation.
                (_, None) => {
                    dtype = None;
                    break;
                }
            }
        } else if !obj.is_none() {
            // Non-image types; short-circuit since only image types are supported and union dtypes are not (yet)
            // supported.
            dtype = None;
            break;
        }
    }
    Ok(dtype)
}
