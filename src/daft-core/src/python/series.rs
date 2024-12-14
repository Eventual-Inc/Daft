use std::{
    hash::BuildHasherDefault,
    ops::{Add, Div, Mul, Rem, Sub},
};

use common_arrow_ffi as ffi;
use daft_hash::{HashFunctionKind, MurBuildHasher, Sha1Hasher};
use daft_schema::python::PyDataType;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    pyclass::CompareOp,
    types::{PyBytes, PyList},
};

use crate::{
    array::{
        ops::{
            as_arrow::AsArrow, trigonometry::TrigonometricFunction, DaftLogical,
            Utf8NormalizeOptions,
        },
        pseudo_arrow::PseudoArrowArray,
        DataArray,
    },
    count_mode::CountMode,
    datatypes::{DataType, Field, ImageMode, PythonType},
    series::{self, IntoSeries, Series},
    utils::arrow::{cast_array_for_daft_if_needed, cast_array_from_daft_if_needed},
};

#[pyclass]
#[derive(Clone)]
pub struct PySeries {
    pub series: series::Series,
}

#[pymethods]
impl PySeries {
    #[staticmethod]
    pub fn from_arrow(py: Python, name: &str, pyarrow_array: Bound<PyAny>) -> PyResult<Self> {
        let arrow_array = ffi::array_to_rust(py, pyarrow_array)?;

        py.allow_threads(|| {
            let arrow_array = cast_array_for_daft_if_needed(arrow_array.to_boxed());
            let series = series::Series::try_from((name, arrow_array))?;
            Ok(series.into())
        })
    }

    // This ingests a Python list[object] directly into a Rust PythonArray.
    #[staticmethod]
    pub fn from_pylist(name: &str, pylist: Bound<PyAny>, pyobj: &str) -> PyResult<Self> {
        let vec_pyobj: Vec<PyObject> = pylist.extract()?;
        let py = pylist.py();
        let dtype = match pyobj {
            "force" => DataType::Python,
            "allow" => infer_daft_dtype_for_sequence(&vec_pyobj, py, name)?.unwrap_or(DataType::Python),
            "disallow" => panic!("Cannot create a Series from a pylist and being strict about only using Arrow types by setting pyobj=disallow"),
            _ => panic!("Unsupported pyobj behavior when creating Series from pylist: {}", pyobj)
        };

        py.allow_threads(|| {
            let arrow_array: Box<dyn arrow2::array::Array> =
                Box::new(PseudoArrowArray::<PyObject>::from_pyobj_vec(vec_pyobj));
            let field = Field::new(name, DataType::Python);

            let data_array = DataArray::<PythonType>::new(field.into(), arrow_array)?;
            let series = data_array.cast(&dtype)?;
            Ok(series.into())
        })
    }

    // This is for PythonArrays only,
    // to convert the Rust PythonArray to a Python list[object].
    pub fn to_pylist(&self) -> PyResult<PyObject> {
        let pseudo_arrow_array = self.series.python()?.as_arrow();
        let pyobj_vec = pseudo_arrow_array.to_pyobj_vec();
        Python::with_gil(|py| Ok(PyList::new_bound(py, pyobj_vec).into()))
    }

    pub fn to_arrow(&self, py: Python) -> PyResult<PyObject> {
        let arrow_array = py.allow_threads(|| {
            let arrow_array = self.series.to_arrow();
            cast_array_from_daft_if_needed(arrow_array)
        });
        Python::with_gil(|py| {
            let pyarrow = py.import_bound(pyo3::intern!(py, "pyarrow"))?;
            Ok(ffi::to_py_array(py, arrow_array, &pyarrow)?.unbind())
        })
    }

    pub fn __abs__(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.abs()?.into()))
    }

    pub fn __add__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok((&self.series).add(&other.series)?.into()))
    }

    pub fn __sub__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok((&self.series).sub(&other.series)?.into()))
    }

    pub fn __mul__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok((&self.series).mul(&other.series)?.into()))
    }

    pub fn __truediv__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok((&self.series).div(&other.series)?.into()))
    }

    pub fn __mod__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok((&self.series).rem(&other.series)?.into()))
    }

    pub fn __and__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.and(&other.series)?.into()))
    }

    pub fn __or__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.or(&other.series)?.into()))
    }

    pub fn __xor__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.xor(&other.series)?.into()))
    }

    pub fn __lshift__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.shift_left(&other.series)?.into()))
    }

    pub fn __rshift__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.shift_right(&other.series)?.into()))
    }

    pub fn __floordiv__(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.floor_div(&other.series)?.into()))
    }

    pub fn ceil(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.ceil()?.into()))
    }

    pub fn floor(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.floor()?.into()))
    }

    pub fn sign(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.sign()?.into()))
    }

    pub fn round(&self, py: Python, decimal: i32) -> PyResult<Self> {
        py.allow_threads(|| {
            if decimal < 0 {
                return Err(PyValueError::new_err(format!(
                    "decimal value can not be negative: {decimal}"
                )));
            }
            Ok(self.series.round(decimal)?.into())
        })
    }
    pub fn clip(&self, py: Python, min: &Self, max: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.clip(&min.series, &max.series)?.into()))
    }

    pub fn sqrt(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.sqrt()?.into()))
    }

    pub fn cbrt(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.cbrt()?.into()))
    }

    pub fn sin(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::Sin)?
                .into())
        })
    }

    pub fn cos(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::Cos)?
                .into())
        })
    }

    pub fn tan(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::Tan)?
                .into())
        })
    }

    pub fn cot(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::Cot)?
                .into())
        })
    }

    pub fn arcsin(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::ArcSin)?
                .into())
        })
    }

    pub fn arccos(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::ArcCos)?
                .into())
        })
    }

    pub fn arctan(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::ArcTan)?
                .into())
        })
    }

    pub fn arctan2(&self, py: Python, other: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.atan2(&other.series)?.into()))
    }

    pub fn degrees(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::Degrees)?
                .into())
        })
    }

    pub fn radians(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::Radians)?
                .into())
        })
    }

    pub fn arctanh(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::ArcTanh)?
                .into())
        })
    }

    pub fn arccosh(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::ArcCosh)?
                .into())
        })
    }

    pub fn arcsinh(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .trigonometry(&TrigonometricFunction::ArcSinh)?
                .into())
        })
    }

    pub fn log2(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.log2()?.into()))
    }

    pub fn log10(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.log10()?.into()))
    }

    pub fn log(&self, py: Python, base: f64) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.log(base)?.into()))
    }

    pub fn ln(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.ln()?.into()))
    }

    pub fn exp(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.exp()?.into()))
    }

    pub fn take(&self, py: Python, idx: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.take(&idx.series)?.into()))
    }

    pub fn slice(&self, py: Python, start: i64, end: i64) -> PyResult<Self> {
        py.allow_threads(|| {
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
        })
    }

    pub fn filter(&self, py: Python, mask: &Self) -> PyResult<Self> {
        if mask.series.data_type() != &DataType::Boolean {
            return Err(PyValueError::new_err(format!(
                "We can only filter a Series with Boolean Series, got {}",
                mask.series.data_type()
            )));
        }
        py.allow_threads(|| Ok(self.series.filter(mask.series.downcast()?)?.into()))
    }

    pub fn sort(&self, py: Python, descending: bool, nulls_first: bool) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.sort(descending, nulls_first)?.into()))
    }

    pub fn argsort(&self, py: Python, descending: bool, nulls_first: bool) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.argsort(descending, nulls_first)?.into()))
    }

    pub fn hash(&self, py: Python, seed: Option<Self>) -> PyResult<Self> {
        py.allow_threads(|| {
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
        })
    }

    pub fn minhash(
        &self,
        py: Python,
        num_hashes: i64,
        ngram_size: i64,
        seed: i64,
        hash_function: &str,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let hash_function: HashFunctionKind = hash_function.parse()?;

            if num_hashes <= 0 {
                return Err(PyValueError::new_err(format!(
                    "num_hashes must be positive: {num_hashes}"
                )));
            }
            if ngram_size <= 0 {
                return Err(PyValueError::new_err(format!(
                    "ngram_size must be positive: {ngram_size}"
                )));
            }

            let seed = seed as u32;

            let num_hashes = num_hashes as usize;
            let ngram_size = ngram_size as usize;

            let result = match hash_function {
                HashFunctionKind::MurmurHash3 => {
                    let hasher = MurBuildHasher::new(seed);
                    self.series.minhash(num_hashes, ngram_size, seed, &hasher)
                }
                HashFunctionKind::XxHash => {
                    let hasher = xxhash_rust::xxh64::Xxh64Builder::new(seed as u64);
                    self.series.minhash(num_hashes, ngram_size, seed, &hasher)
                }
                HashFunctionKind::Sha1 => {
                    let hasher = BuildHasherDefault::<Sha1Hasher>::default();
                    self.series.minhash(num_hashes, ngram_size, seed, &hasher)
                }
            }?;

            Ok(result.into())
        })
    }

    pub fn __richcmp__(&self, py: Python, other: &Self, op: CompareOp) -> PyResult<Self> {
        py.allow_threads(|| {
            use crate::array::ops::DaftCompare;
            match op {
                CompareOp::Lt => Ok((self.series.lt(&other.series)?).into_series().into()),
                CompareOp::Le => Ok((self.series.lte(&other.series)?).into_series().into()),
                CompareOp::Eq => Ok((self.series.equal(&other.series)?).into_series().into()),
                CompareOp::Ne => Ok((self.series.not_equal(&other.series)?).into_series().into()),
                CompareOp::Gt => Ok((self.series.gt(&other.series)?).into_series().into()),
                CompareOp::Ge => Ok((self.series.gte(&other.series)?).into_series().into()),
            }
        })
    }

    pub fn __invert__(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| {
            use std::ops::Not;
            Ok((&self.series).not()?.into())
        })
    }

    pub fn count(&self, py: Python, mode: CountMode) -> PyResult<Self> {
        py.allow_threads(|| Ok((self.series).count(None, mode)?.into()))
    }

    pub fn sum(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok((self.series).sum(None)?.into()))
    }

    pub fn mean(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok((self.series).mean(None)?.into()))
    }

    pub fn min(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok((self.series).min(None)?.into()))
    }

    pub fn max(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok((self.series).max(None)?.into()))
    }

    pub fn shift_left(&self, py: Python, bits: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.shift_left(&bits.series)?.into()))
    }

    pub fn shift_right(&self, py: Python, bits: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.shift_right(&bits.series)?.into()))
    }

    pub fn agg_list(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok((self.series).agg_list(None)?.into()))
    }

    pub fn cast(&self, py: Python, dtype: PyDataType) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.cast(&dtype.into())?.into()))
    }

    #[staticmethod]
    pub fn concat(py: Python, series: Vec<Self>) -> PyResult<Self> {
        py.allow_threads(|| {
            let series: Vec<_> = series.iter().map(|s| &s.series).collect();
            Ok(Series::concat(series.as_slice())?.into())
        })
    }

    pub fn __repr__(&self, py: Python) -> PyResult<String> {
        py.allow_threads(|| Ok(format!("{}", self.series)))
    }

    pub fn __len__(&self, py: Python) -> PyResult<usize> {
        py.allow_threads(|| Ok(self.series.len()))
    }

    pub fn size_bytes(&self, py: Python) -> PyResult<usize> {
        py.allow_threads(|| Ok(self.series.size_bytes()?))
    }

    pub fn name(&self, py: Python) -> PyResult<String> {
        py.allow_threads(|| Ok(self.series.name().to_string()))
    }

    pub fn rename(&self, py: Python, name: &str) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.rename(name).into()))
    }

    pub fn data_type(&self, py: Python) -> PyResult<PyDataType> {
        py.allow_threads(|| Ok(self.series.data_type().clone().into()))
    }

    pub fn utf8_endswith(&self, py: Python, pattern: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_endswith(&pattern.series)?.into()))
    }

    pub fn utf8_startswith(&self, py: Python, pattern: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_startswith(&pattern.series)?.into()))
    }

    pub fn utf8_contains(&self, py: Python, pattern: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_contains(&pattern.series)?.into()))
    }

    pub fn utf8_match(&self, py: Python, pattern: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_match(&pattern.series)?.into()))
    }

    pub fn utf8_split(&self, py: Python, pattern: &Self, regex: bool) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_split(&pattern.series, regex)?.into()))
    }

    pub fn utf8_extract(&self, py: Python, pattern: &Self, index: usize) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_extract(&pattern.series, index)?.into()))
    }

    pub fn utf8_extract_all(&self, py: Python, pattern: &Self, index: usize) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_extract_all(&pattern.series, index)?.into()))
    }

    pub fn utf8_replace(
        &self,
        py: Python,
        pattern: &Self,
        replacement: &Self,
        regex: bool,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .utf8_replace(&pattern.series, &replacement.series, regex)?
                .into())
        })
    }

    pub fn utf8_length(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_length()?.into()))
    }

    pub fn utf8_length_bytes(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_length_bytes()?.into()))
    }

    pub fn utf8_lower(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_lower()?.into()))
    }

    pub fn utf8_upper(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_upper()?.into()))
    }

    pub fn utf8_lstrip(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_lstrip()?.into()))
    }

    pub fn utf8_rstrip(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_rstrip()?.into()))
    }

    pub fn utf8_reverse(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_reverse()?.into()))
    }

    pub fn utf8_capitalize(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_capitalize()?.into()))
    }

    pub fn utf8_left(&self, py: Python, nchars: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_left(&nchars.series)?.into()))
    }

    pub fn utf8_right(&self, py: Python, nchars: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_right(&nchars.series)?.into()))
    }

    pub fn utf8_find(&self, py: Python, substr: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_find(&substr.series)?.into()))
    }

    pub fn utf8_rpad(&self, py: Python, length: &Self, character: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .utf8_rpad(&length.series, &character.series)?
                .into())
        })
    }

    pub fn utf8_lpad(&self, py: Python, length: &Self, character: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .utf8_lpad(&length.series, &character.series)?
                .into())
        })
    }

    pub fn utf8_repeat(&self, py: Python, n: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_repeat(&n.series)?.into()))
    }

    pub fn utf8_like(&self, py: Python, pattern: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_like(&pattern.series)?.into()))
    }

    pub fn utf8_ilike(&self, py: Python, pattern: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_ilike(&pattern.series)?.into()))
    }

    pub fn utf8_substr(&self, py: Python, start: &Self, length: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .utf8_substr(&start.series, &length.series)?
                .into())
        })
    }

    pub fn utf8_to_date(&self, py: Python, format: &str) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_to_date(format)?.into()))
    }

    pub fn utf8_to_datetime(
        &self,
        py: Python,
        format: &str,
        timezone: Option<&str>,
    ) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.utf8_to_datetime(format, timezone)?.into()))
    }

    pub fn utf8_normalize(
        &self,
        py: Python,
        remove_punct: bool,
        lowercase: bool,
        nfd_unicode: bool,
        white_space: bool,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let opts = Utf8NormalizeOptions {
                remove_punct,
                lowercase,
                nfd_unicode,
                white_space,
            };

            Ok(self.series.utf8_normalize(opts)?.into())
        })
    }

    pub fn utf8_count_matches(
        &self,
        py: Python,
        patterns: &Self,
        whole_word: bool,
        case_sensitive: bool,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .utf8_count_matches(&patterns.series, whole_word, case_sensitive)?
                .into())
        })
    }

    pub fn is_nan(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.is_nan()?.into()))
    }

    pub fn is_inf(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.is_inf()?.into()))
    }

    pub fn not_nan(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.not_nan()?.into()))
    }

    pub fn fill_nan(&self, py: Python, fill_value: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.fill_nan(&fill_value.series)?.into()))
    }

    pub fn dt_date(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_date()?.into()))
    }

    pub fn dt_day(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_day()?.into()))
    }

    pub fn dt_hour(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_hour()?.into()))
    }

    pub fn dt_minute(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_minute()?.into()))
    }

    pub fn dt_second(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_second()?.into()))
    }

    pub fn dt_time(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_time()?.into()))
    }

    pub fn dt_month(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_month()?.into()))
    }

    pub fn dt_year(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_year()?.into()))
    }

    pub fn dt_day_of_week(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.dt_day_of_week()?.into()))
    }

    pub fn dt_truncate(&self, py: Python, interval: &str, relative_to: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .dt_truncate(interval, &relative_to.series)?
                .into())
        })
    }

    pub fn partitioning_days(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.partitioning_days()?.into()))
    }

    pub fn partitioning_hours(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.partitioning_hours()?.into()))
    }

    pub fn partitioning_months(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.partitioning_months()?.into()))
    }

    pub fn partitioning_years(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.partitioning_years()?.into()))
    }

    pub fn partitioning_iceberg_bucket(&self, py: Python, n: i32) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.partitioning_iceberg_bucket(n)?.into()))
    }

    pub fn partitioning_iceberg_truncate(&self, py: Python, w: i64) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.partitioning_iceberg_truncate(w)?.into()))
    }

    pub fn murmur3_32(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.murmur3_32()?.into_series().into()))
    }

    pub fn list_count(&self, py: Python, mode: CountMode) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.list_count(mode)?.into_series().into()))
    }

    pub fn list_get(&self, py: Python, idx: &Self, default: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.list_get(&idx.series, &default.series)?.into()))
    }

    pub fn list_slice(&self, py: Python, start: &Self, end: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.list_slice(&start.series, &end.series)?.into()))
    }

    pub fn list_sort(&self, py: Python, desc: &Self, nulls_first: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .list_sort(&desc.series, &nulls_first.series)?
                .into())
        })
    }

    pub fn map_get(&self, py: Python, key: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.map_get(&key.series)?.into()))
    }

    pub fn if_else(&self, py: Python, other: &Self, predicate: &Self) -> PyResult<Self> {
        py.allow_threads(|| {
            Ok(self
                .series
                .if_else(&other.series, &predicate.series)?
                .into())
        })
    }

    pub fn is_null(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.is_null()?.into()))
    }

    pub fn not_null(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.not_null()?.into()))
    }

    pub fn fill_null(&self, py: Python, fill_value: &Self) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.fill_null(&fill_value.series)?.into()))
    }

    pub fn _debug_bincode_serialize(&self, py: Python) -> PyResult<PyObject> {
        let values = py.allow_threads(|| bincode::serialize(&self.series).unwrap());
        Ok(PyBytes::new_bound(py, &values).into())
    }

    #[staticmethod]
    pub fn _debug_bincode_deserialize(py: Python, bytes: &[u8]) -> PyResult<Self> {
        py.allow_threads(|| {
            let values = bincode::deserialize::<Series>(bytes).unwrap();
            Ok(Self { series: values })
        })
    }

    pub fn to_str_values(&self, py: Python) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.series.to_str_values()?.into()))
    }
}

impl From<series::Series> for PySeries {
    fn from(value: series::Series) -> Self {
        Self { series: value }
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
        .import_bound(pyo3::intern!(py, "PIL.Image"))
        .and_then(|m| m.getattr(pyo3::intern!(py, "Image")));
    let np_ndarray_type = py
        .import_bound(pyo3::intern!(py, "numpy"))
        .and_then(|m| m.getattr(pyo3::intern!(py, "ndarray")));
    let np_generic_type = py
        .import_bound(pyo3::intern!(py, "numpy"))
        .and_then(|m| m.getattr(pyo3::intern!(py, "generic")));
    let from_numpy_dtype = {
        py.import_bound(pyo3::intern!(py, "daft.datatype"))?
            .getattr(pyo3::intern!(py, "DataType"))?
            .getattr(pyo3::intern!(py, "from_numpy_dtype"))?
    };
    let mut dtype: Option<DataType> = None;
    for obj in vec_pyobj {
        let obj = obj.bind(py);
        if let Ok(pil_image_type) = &py_pil_image_type
            && obj.is_instance(pil_image_type)?
        {
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
        } else if let Ok(np_ndarray_type) = &np_ndarray_type
            && let Ok(np_generic_type) = &np_generic_type
            && (obj.is_instance(np_ndarray_type)? || obj.is_instance(np_generic_type)?)
        {
            let np_dtype = obj.getattr(pyo3::intern!(py, "dtype"))?;
            let inferred_inner_dtype = from_numpy_dtype.call1((np_dtype,)).map(|dt| {
                dt.getattr(pyo3::intern!(py, "_dtype"))
                    .unwrap()
                    .extract::<PyDataType>()
                    .unwrap()
                    .dtype
            });
            let shape: Vec<u64> = obj.getattr(pyo3::intern!(py, "shape"))?.extract()?;
            let inferred_dtype = match inferred_inner_dtype {
                Ok(inferred_inner_dtype) if shape.len() == 1 => {
                    Some(DataType::List(Box::new(inferred_inner_dtype)))
                }
                Ok(inferred_inner_dtype) if shape.len() > 1 => {
                    Some(DataType::Tensor(Box::new(inferred_inner_dtype)))
                }
                _ => None,
            };
            match (&dtype, &inferred_dtype) {
                // Tensors with mixed inner dtypes is not supported, short-circuit.
                (Some(existing_dtype), Some(inferred_dtype))
                    if existing_dtype != inferred_dtype =>
                {
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
