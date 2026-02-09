use std::{
    ops::{Add, Div, Mul, Rem, Sub},
    sync::Arc,
};

use arrow::array::make_array;
use common_arrow_ffi::{ToPyArrow, array_to_rust};
use common_error::DaftError;
use daft_hash::HashFunctionKind;
use daft_schema::python::PyDataType;
use pyo3::{
    exceptions::{PyIndexError, PyStopIteration, PyValueError},
    ffi::Py_uintptr_t,
    prelude::*,
    pyclass::CompareOp,
    types::{PyBytes, PyList},
};

use crate::{
    array::ops::DaftLogical,
    count_mode::CountMode,
    datatypes::{DataType, Field},
    lit::Literal,
    prelude::PythonArray,
    series::{
        self, IntoSeries, Series,
        from_lit::{combine_lit_types, series_from_literals_iter},
    },
    utils::supertype::try_get_collection_supertype,
};

#[pyclass]
#[derive(Clone)]
pub struct PySeries {
    pub series: series::Series,
}
impl PySeries {
    pub fn from_pylist_impl(
        name: &str,
        vec_pyobj: Vec<Bound<PyAny>>,
        dtype: DataType,
    ) -> PyResult<Self> {
        let pyobjs_arced = vec_pyobj.into_iter().map(|obj| {
            if obj.is_none() {
                None
            } else {
                Some(Arc::new(obj.unbind()))
            }
        });
        let arr = PythonArray::from_iter(name, pyobjs_arced);
        let series = arr.cast(&dtype)?;

        Ok(series.into())
    }
}

#[pymethods]
impl PySeries {
    #[staticmethod]
    #[pyo3(signature = (name, pyarrow_array, dtype=None))]
    pub fn from_arrow(
        name: &str,
        pyarrow_array: Bound<PyAny>,
        dtype: Option<PyDataType>,
    ) -> PyResult<Self> {
        let (data, field) = array_to_rust(&pyarrow_array)?;
        let daft_field = daft_schema::field::Field::try_from(&field)?.rename(name);

        // For Extension types, get the coerced inner storage type directly
        // (e.g. Binary → LargeBinary). We can't use Field::to_arrow() here because
        // it uses the REGISTRY to return the *original* storage type for export,
        // but internally we need the coerced type.
        // For all other types, use Field::to_arrow() which handles logical types
        // like Embedding, Tensor, Image correctly.
        let target_arrow_dtype = match &daft_field.dtype {
            DataType::Extension(_, inner_dtype, _) => inner_dtype.to_arrow()?,
            _ => daft_field.to_arrow()?.data_type().clone(),
        };

        let arr = make_array(data);
        let arr = if &target_arrow_dtype != field.data_type() {
            arrow::compute::cast(&arr, &target_arrow_dtype).map_err(DaftError::from)?
        } else {
            arr
        };

        let series = if let Some(dtype) = dtype {
            let field = Field::new(name, dtype.into());
            series::Series::from_arrow(Arc::new(field), arr)?
        } else {
            series::Series::from_arrow(Arc::new(daft_field), arr)?
        };

        Ok(series.into())
    }

    #[staticmethod]
    #[pyo3(signature = (list, name=None, dtype=None))]
    pub fn from_pylist(
        list: &Bound<PyList>,
        name: Option<&str>,
        dtype: Option<PyDataType>,
    ) -> PyResult<Self> {
        let dtype = dtype.map(|t| t.dtype);
        let mut series = if let Some(dtype) = dtype {
            let literals = (0..list.len()).map(|i| {
                list.get_item(i)
                    .and_then(|elem| Literal::from_pyobj(&elem, Some(&dtype)))
                    .map_err(DaftError::from)
            });
            series_from_literals_iter(literals, Some(dtype.clone()))?
        } else {
            let literals = list
                .iter()
                .map(|elem| Literal::from_pyobj(&elem, None))
                .collect::<PyResult<Vec<_>>>()?;

            let supertype = try_get_collection_supertype(literals.iter().map(Literal::get_type))
                .unwrap_or(DataType::Python);

            let literals_with_supertype = literals.into_iter().enumerate().map(|(i, daft_lit)| {
                if combine_lit_types(&daft_lit.get_type(), &supertype).as_ref() == Some(&supertype)
                {
                    Ok(daft_lit)
                } else {
                    let py_lit = list.get_item(i)?;

                    // if literal doesn't match supertype, redo conversion so that for the python data type
                    // as well as nested types with python type, we avoid any lossy conversions and just keep
                    // stuff as Python objects
                    Literal::from_pyobj(&py_lit, Some(&supertype)).map_err(DaftError::from)
                }
            });
            series_from_literals_iter(literals_with_supertype, Some(supertype.clone()))?
        };

        if let Some(name) = name {
            series = series.rename(name);
        }
        Ok(series.into())
    }

    pub fn to_pylist<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyList>> {
        PyList::new(py, self.series.to_literals())
    }

    pub fn to_arrow<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        self.series.to_pyarrow(py)
    }

    pub fn __iter__(&self) -> PySeriesIterator {
        PySeriesIterator::new(self.series.clone())
    }

    pub fn __getitem__(&self, index: usize) -> PyResult<Literal> {
        let length = self.series.len();
        if index >= length {
            Err(PyIndexError::new_err(format!(
                "Index out of range for series of length {length}: {index}"
            )))
        } else {
            Ok(self.series.get_lit(index))
        }
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
        Ok(self.series.and(&other.series)?.into())
    }

    pub fn __or__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.or(&other.series)?.into())
    }

    pub fn __xor__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.xor(&other.series)?.into())
    }

    pub fn __lshift__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.shift_left(&other.series)?.into())
    }

    pub fn __rshift__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.shift_right(&other.series)?.into())
    }

    pub fn __floordiv__(&self, other: &Self) -> PyResult<Self> {
        Ok(self.series.floor_div(&other.series)?.into())
    }

    pub fn log2(&self) -> PyResult<Self> {
        Ok(self.series.log2()?.into())
    }

    pub fn log10(&self) -> PyResult<Self> {
        Ok(self.series.log10()?.into())
    }

    pub fn log(&self, base: f64) -> PyResult<Self> {
        Ok(self.series.log(base)?.into())
    }

    pub fn ln(&self) -> PyResult<Self> {
        Ok(self.series.ln()?.into())
    }

    pub fn log1p(&self) -> PyResult<Self> {
        Ok(self.series.log1p()?.into())
    }

    pub fn take(&self, idx: &Self) -> PyResult<Self> {
        let idx = idx.series.cast(&DataType::UInt64)?;
        Ok(self.series.take(idx.u64()?)?.into())
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

    pub fn sort(&self, descending: bool, nulls_first: bool) -> PyResult<Self> {
        Ok(self.series.sort(descending, nulls_first)?.into())
    }

    pub fn argsort(&self, descending: bool, nulls_first: bool) -> PyResult<Self> {
        Ok(self
            .series
            .argsort(descending, nulls_first)?
            .into_series()
            .into())
    }

    pub fn minhash(
        &self,
        num_hashes: i64,
        ngram_size: i64,
        seed: i64,
        hash_function: &str,
    ) -> PyResult<Self> {
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

        let result = self
            .series
            .minhash(num_hashes, ngram_size, seed, hash_function)?;
        Ok(result.into())
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

    pub fn count(&self, mode: CountMode) -> PyResult<Self> {
        Ok((self.series).count(None, mode)?.into())
    }

    pub fn sum(&self) -> PyResult<Self> {
        Ok((self.series).sum(None)?.into())
    }

    pub fn product(&self) -> PyResult<Self> {
        Ok((self.series).product(None)?.into())
    }

    pub fn mean(&self) -> PyResult<Self> {
        Ok((self.series).mean(None)?.into())
    }

    #[pyo3(signature = (ddof=0))]
    pub fn stddev(&self, ddof: usize) -> PyResult<Self> {
        Ok((self.series).stddev(None, ddof)?.into())
    }

    pub fn min(&self) -> PyResult<Self> {
        Ok((self.series).min(None)?.into())
    }

    pub fn max(&self) -> PyResult<Self> {
        Ok((self.series).max(None)?.into())
    }

    pub fn shift_left(&self, bits: &Self) -> PyResult<Self> {
        Ok(self.series.shift_left(&bits.series)?.into())
    }

    pub fn shift_right(&self, bits: &Self) -> PyResult<Self> {
        Ok(self.series.shift_right(&bits.series)?.into())
    }

    pub fn agg_list(&self) -> PyResult<Self> {
        Ok((self.series).agg_list(None)?.into())
    }

    pub fn agg_set(&self) -> PyResult<Self> {
        Ok((self.series).agg_set(None)?.into())
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

    pub fn size_bytes(&self) -> usize {
        self.series.size_bytes()
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

    pub fn partitioning_days(&self) -> PyResult<Self> {
        Ok(self.series.partitioning_days()?.into())
    }

    pub fn partitioning_hours(&self) -> PyResult<Self> {
        Ok(self.series.partitioning_hours()?.into())
    }

    pub fn partitioning_months(&self) -> PyResult<Self> {
        Ok(self.series.partitioning_months()?.into())
    }

    pub fn partitioning_years(&self) -> PyResult<Self> {
        Ok(self.series.partitioning_years()?.into())
    }

    pub fn partitioning_iceberg_bucket(&self, n: i32) -> PyResult<Self> {
        Ok(self.series.partitioning_iceberg_bucket(n)?.into())
    }

    pub fn partitioning_iceberg_truncate(&self, w: i64) -> PyResult<Self> {
        Ok(self.series.partitioning_iceberg_truncate(w)?.into())
    }

    pub fn murmur3_32(&self) -> PyResult<Self> {
        Ok(self.series.murmur3_32()?.into_series().into())
    }

    pub fn map_get(&self, key: &Self) -> PyResult<Self> {
        Ok(self.series.map_get(&key.series)?.into())
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

    pub fn not_null(&self) -> PyResult<Self> {
        Ok(self.series.not_null()?.into())
    }

    pub fn fill_null(&self, fill_value: &Self) -> PyResult<Self> {
        Ok(self.series.fill_null(&fill_value.series)?.into())
    }

    pub fn _debug_bincode_serialize(&self, py: Python) -> PyResult<Py<PyAny>> {
        let values =
            bincode::serde::encode_to_vec(&self.series, bincode::config::legacy()).unwrap();
        Ok(PyBytes::new(py, &values).into())
    }

    #[staticmethod]
    pub fn _debug_bincode_deserialize(bytes: &[u8]) -> PyResult<Self> {
        let values: Series = bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
            .unwrap()
            .0;
        Ok(Self { series: values })
    }

    pub fn to_str_values(&self) -> PyResult<Self> {
        Ok(self.series.to_str_values()?.into())
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

#[pyclass]
#[derive(Clone)]
/// Iterator over elements in a Python Series
///
/// Implemented in Rust so that we can more easily optimize it in the future.
pub struct PySeriesIterator {
    series: series::Series,
    idx: usize,
}

impl PySeriesIterator {
    fn new(series: series::Series) -> Self {
        Self { series, idx: 0 }
    }
}

#[pymethods]
impl PySeriesIterator {
    fn __next__(&mut self) -> PyResult<Literal> {
        if self.idx == self.series.len() {
            Err(PyStopIteration::new_err(()))
        } else {
            let val = self.series.get_lit(self.idx);
            self.idx += 1;
            Ok(val)
        }
    }

    fn __iter__(&self) -> Self {
        self.clone()
    }
}

impl ToPyArrow for Series {
    fn to_pyarrow<'py>(
        &self,
        py: pyo3::Python<'py>,
    ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let array = self.to_arrow()?;
        let target_field = self.field().to_arrow()?;
        let array = if array.data_type() != target_field.data_type() {
            arrow::compute::cast(&array, target_field.data_type()).map_err(DaftError::from)?
        } else {
            array
        };

        let schema = Box::new(arrow::ffi::FFI_ArrowSchema::try_from(target_field).map_err(
            |e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to convert Arrow field to FFI schema: {}",
                    e
                ))
            },
        )?);

        let mut data = array.to_data();
        data.align_buffers();
        let arrow_arr = Box::new(arrow::ffi::FFI_ArrowArray::new(&data));

        let schema_ptr: *const arrow::ffi::FFI_ArrowSchema = &raw const *schema;
        let array_ptr: *const arrow::ffi::FFI_ArrowArray = &raw const *arrow_arr;

        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        let array = pyarrow.getattr(pyo3::intern!(py, "Array"))?.call_method1(
            pyo3::intern!(py, "_import_from_c"),
            (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
        )?;

        let array = pyo3::types::PyModule::import(py, pyo3::intern!(py, "daft.arrow_utils"))?
            .getattr(pyo3::intern!(py, "remove_empty_struct_placeholders"))?
            .call1((array,))?;

        Ok(array)
    }
}
