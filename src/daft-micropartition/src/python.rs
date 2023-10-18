#![allow(unused)] // MAKE SURE TO REMOVE THIS

use std::sync::{Arc, Mutex};

use common_error::DaftResult;
use daft_core::python::{datatype::PyTimeUnit, schema::PySchema, PySeries};
use daft_dsl::python::PyExpr;
use daft_io::{get_io_client, python::IOConfig, IOStatsContext};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_table::python::PyTable;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyDict, PyList},
    Python,
};

use crate::micropartition::MicroPartition;

#[pyclass(module = "daft.daft")]
#[derive(Clone)]
struct PyMicroPartition {
    inner: Arc<MicroPartition>,
}

#[pymethods]
impl PyMicroPartition {
    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(PySchema {
            schema: self.inner.schema.clone(),
        })
    }

    pub fn column_names(&self) -> PyResult<Vec<String>> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn get_column(&self) -> PyResult<PySeries> {
        /// We can prob skip this one since we only use it for tests
        todo!("[MICROPARTITION_INT]")
    }

    pub fn size_bytes(&self) -> PyResult<usize> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.inner.len())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }

    pub fn __repr_html__(&self) -> PyResult<String> {
        todo!("[MICROPARTITION_INT]")
    }

    // Creation Methods
    #[staticmethod]
    pub fn empty(schema: Option<PySchema>) -> PyResult<Self> {
        todo!("[MICROPARTITION_INT]")
    }

    #[staticmethod]
    pub fn from_arrow(arrow_table: PyObject) -> PyResult<Self> {
        // maybe should be in python side: micropartition.py
        todo!("[MICROPARTITION_INT]")
    }

    #[staticmethod]
    pub fn from_arrow_record_batches(record_batches: PyObject) -> PyResult<Self> {
        // this can probably be smarter since we don't have to concat anymore
        todo!("[MICROPARTITION_INT]")
    }

    #[staticmethod]
    pub fn from_pandas(pd_df: PyObject) -> PyResult<Self> {
        // maybe should be in python side: micropartition.py
        todo!("[MICROPARTITION_INT]")
    }

    #[staticmethod]
    pub fn from_pydict(data: PyObject) -> PyResult<Self> {
        // maybe should be in python side: micropartition.py
        todo!("[MICROPARTITION_INT]")
    }

    // Exporting Methods

    pub fn to_arrow(
        &self,
        cast_tensors_to_ray_tensor_dtype: Option<bool>,
        convert_large_arrays: Option<bool>,
    ) -> PyResult<PyObject> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn to_pydict(&self) -> PyResult<PyObject> {
        // maybe should be in python side: micropartition.py

        todo!("[MICROPARTITION_INT]")
    }

    pub fn to_pylist(&self) -> PyResult<PyObject> {
        // maybe should be in python side: micropartition.py
        todo!("[MICROPARTITION_INT]")
    }

    pub fn to_pandas(&self, cast_tensors_to_ray_tensor_dtype: Option<bool>) -> PyResult<PyObject> {
        // maybe should be in python side: micropartition.py
        todo!("[MICROPARTITION_INT]")
    }

    // Compute Methods

    #[staticmethod]
    pub fn concat(py: Python, to_concat: Vec<Self>) -> PyResult<Self> {
        let mps: Vec<_> = to_concat.iter().map(|t| t.inner.as_ref()).collect();
        py.allow_threads(|| Ok(MicroPartition::concat(mps.as_slice())?.into()))
    }

    pub fn slice(&self, py: Python, start: i64, end: i64) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.slice(start as usize, end as usize)?.into()))
    }

    pub fn cast_to_schema(&self, py: Python, schema: PySchema) -> PyResult<Self> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn eval_expression_list(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn take(&self, py: Python, idx: &PySeries) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.take(&idx.series)?.into()))
    }

    pub fn filter(&mut self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| Ok(self.inner.filter(converted_exprs.as_slice())?.into()))
    }

    pub fn sort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> =
            sort_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .sort(converted_exprs.as_slice(), descending.as_slice())?
                .into())
        })
    }

    pub fn argsort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<PySeries> {
        let converted_exprs: Vec<daft_dsl::Expr> =
            sort_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .argsort(converted_exprs.as_slice(), descending.as_slice())?
                .into())
        })
    }

    pub fn agg(&self, py: Python, to_agg: Vec<PyExpr>, group_by: Vec<PyExpr>) -> PyResult<Self> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn join(
        &self,
        py: Python,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
    ) -> PyResult<Self> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn explode(&self, py: Python, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        todo!("[MICROPARTITION_INT]")
    }

    pub fn head(&self, py: Python, num: i64) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.head(num as usize)?.into()))
    }

    pub fn sample(&self, py: Python, num: i64) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.sample(num as usize)?.into()))
    }

    pub fn quantiles(&self, py: Python, num: i64) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.quantiles(num as usize)?.into()))
    }

    pub fn partition_by_hash(
        &self,
        py: Python,
        exprs: Vec<PyExpr>,
        num_partitions: i64,
    ) -> PyResult<Vec<Self>> {
        if num_partitions < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not partition into negative number of partitions: {num_partitions}"
            )));
        }
        let exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .partition_by_hash(exprs.as_slice(), num_partitions as usize)?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<Self>>())
        })
    }

    pub fn partition_by_random(
        &self,
        py: Python,
        num_partitions: i64,
        seed: i64,
    ) -> PyResult<Vec<Self>> {
        if num_partitions < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not partition into negative number of partitions: {num_partitions}"
            )));
        }

        if seed < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not have seed has negative number: {seed}"
            )));
        }
        py.allow_threads(|| {
            Ok(self
                .inner
                .partition_by_random(num_partitions as usize, seed as u64)?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<Self>>())
        })
    }

    pub fn partition_by_range(
        &self,
        py: Python,
        partition_keys: Vec<PyExpr>,
        boundaries: &PyTable,
        descending: Vec<bool>,
    ) -> PyResult<Vec<Self>> {
        let exprs: Vec<daft_dsl::Expr> = partition_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .partition_by_range(exprs.as_slice(), &boundaries.table, descending.as_slice())?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<Self>>())
        })
    }

    #[staticmethod]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Self> {
        let mp = py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uri}"));

            let io_config = io_config.unwrap_or_default().config.into();
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );
            // TODO: [MICROPARTITION_INT] PASS THE REST OF THE OPTIONS IN
            crate::micropartition::read_parquet_into_micropartition(
                [uri].as_slice(),
                io_config,
                Some(io_stats),
                multithreaded_io.unwrap_or(true),
            )
        })?;
        Ok(PyMicroPartition {
            inner: Arc::new(mp),
        })
    }
}

impl From<MicroPartition> for PyMicroPartition {
    fn from(value: MicroPartition) -> Self {
        PyMicroPartition {
            inner: Arc::new(value),
        }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyMicroPartition>()?;
    Ok(())
}
