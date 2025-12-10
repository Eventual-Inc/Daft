use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::python::PyOperatorMetrics;
use daft_core::{
    join::JoinType,
    prelude::*,
    python::{PySchema, series::PySeries},
};
use daft_dsl::{
    Expr,
    expr::bound_expr::{BoundAggExpr, BoundExpr},
    python::PyExpr,
};
use indexmap::IndexMap;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};

use crate::{
    RecordBatch, ffi,
    file_info::{FileInfo, FileInfos},
    preview::{Preview, PreviewFormat, PreviewOptions},
};

#[pyclass]
#[derive(Clone)]
pub struct PyRecordBatch {
    pub record_batch: RecordBatch,
}

#[pymethods]
impl PyRecordBatch {
    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(PySchema {
            schema: self.record_batch.schema.clone(),
        })
    }

    pub fn eval_expression_list(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs = BoundExpr::bind_all(&exprs, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .eval_expression_list(converted_exprs.as_slice())?
                .into())
        })
    }

    pub fn eval_expression_list_with_metrics(
        &self,
        py: Python,
        exprs: Vec<PyExpr>,
    ) -> PyResult<(Self, PyOperatorMetrics)> {
        let converted_exprs = BoundExpr::bind_all(&exprs, &self.record_batch.schema)?;
        let record_batch = self.record_batch.clone();
        py.detach(move || {
            let mut metrics_collector = PyOperatorMetrics::new();
            let evaluated = record_batch.eval_expression_list_with_metrics(
                converted_exprs.as_slice(),
                &mut metrics_collector.inner,
            )?;
            PyResult::Ok((evaluated.into(), metrics_collector))
        })
    }

    pub fn take(&self, py: Python, idx: &PySeries) -> PyResult<Self> {
        py.detach(|| {
            let idx = idx.series.cast(&DataType::UInt64)?;
            Ok(self.record_batch.take(idx.u64()?)?.into())
        })
    }

    pub fn filter(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs = BoundExpr::bind_all(&exprs, &self.record_batch.schema)?;
        py.detach(|| Ok(self.record_batch.filter(converted_exprs.as_slice())?.into()))
    }

    pub fn sort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> PyResult<Self> {
        let converted_exprs = BoundExpr::bind_all(&sort_keys, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .sort(
                    converted_exprs.as_slice(),
                    descending.as_slice(),
                    nulls_first.as_slice(),
                )?
                .into())
        })
    }

    pub fn argsort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> PyResult<PySeries> {
        let converted_exprs = BoundExpr::bind_all(&sort_keys, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .argsort(
                    converted_exprs.as_slice(),
                    descending.as_slice(),
                    nulls_first.as_slice(),
                )?
                .into_series()
                .into())
        })
    }

    pub fn agg(&self, py: Python, to_agg: Vec<PyExpr>, group_by: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_agg = BoundExpr::bind_all(&to_agg, &self.record_batch.schema)?
            .into_iter()
            .map(|expr| {
                if let Expr::Agg(agg_expr) = expr.as_ref() {
                    Ok(BoundAggExpr::new_unchecked(agg_expr.clone()))
                } else {
                    Err(DaftError::ValueError(
                        format!("RecordBatch.agg requires all to_agg inputs to be aggregation expressions, found: {expr}"),
                    ))
                }
            })
            .collect::<DaftResult<Vec<_>>>()?;
        let converted_group_by = BoundExpr::bind_all(&group_by, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .agg(converted_to_agg.as_slice(), converted_group_by.as_slice())?
                .into())
        })
    }

    pub fn pivot(
        &self,
        py: Python,
        group_by: Vec<PyExpr>,
        pivot_col: PyExpr,
        values_col: PyExpr,
        names: Vec<String>,
    ) -> PyResult<Self> {
        let converted_group_by = BoundExpr::bind_all(&group_by, &self.record_batch.schema)?;
        let converted_pivot_col = BoundExpr::try_new(pivot_col, &self.record_batch.schema)?;
        let converted_values_col = BoundExpr::try_new(values_col, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .pivot(
                    converted_group_by.as_slice(),
                    converted_pivot_col,
                    converted_values_col,
                    names,
                )?
                .into())
        })
    }

    pub fn hash_join(
        &self,
        py: Python,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        how: JoinType,
    ) -> PyResult<Self> {
        let left_exprs = BoundExpr::bind_all(&left_on, &self.record_batch.schema)?;
        let right_exprs = BoundExpr::bind_all(&right_on, &self.record_batch.schema)?;
        let null_equals_nulls = vec![false; left_exprs.len()];
        py.detach(|| {
            Ok(self
                .record_batch
                .hash_join(
                    &right.record_batch,
                    left_exprs.as_slice(),
                    right_exprs.as_slice(),
                    null_equals_nulls.as_slice(),
                    how,
                )?
                .into())
        })
    }

    pub fn sort_merge_join(
        &self,
        py: Python,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        is_sorted: bool,
    ) -> PyResult<Self> {
        let left_exprs = BoundExpr::bind_all(&left_on, &self.record_batch.schema)?;
        let right_exprs = BoundExpr::bind_all(&right_on, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .sort_merge_join(
                    &right.record_batch,
                    left_exprs.as_slice(),
                    right_exprs.as_slice(),
                    is_sorted,
                )?
                .into())
        })
    }

    pub fn explode(&self, py: Python, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_explode = BoundExpr::bind_all(&to_explode, &self.record_batch.schema)?;

        py.detach(|| {
            Ok(self
                .record_batch
                .explode(converted_to_explode.as_slice())?
                .into())
        })
    }

    /// Helper to create a rust Preview and use its Display impl.
    #[pyo3(signature = (format, options))]
    pub fn preview(&self, format: &str, options: Option<&str>) -> PyResult<String> {
        let format = PreviewFormat::try_from(format)?;
        let options = match options {
            Some(json) => serde_json::from_str::<PreviewOptions>(json)
                .expect("PreviewOptions produced invalid json."),
            None => PreviewOptions::default(),
        };
        let preview = self.record_batch.clone();
        let preview = Preview::new(preview, format, options);
        Ok(preview.to_string())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.record_batch))
    }

    pub fn _repr_html_(&self) -> PyResult<String> {
        Ok(self.record_batch.repr_html())
    }

    pub fn head(&self, py: Python, num: i64) -> PyResult<Self> {
        if num < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not head Table with negative number: {num}"
            )));
        }
        let num = num as usize;
        py.detach(|| Ok(self.record_batch.head(num)?.into()))
    }

    #[pyo3(signature = (fraction, with_replacement, seed=None))]
    pub fn sample_by_fraction(
        &self,
        py: Python,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> PyResult<Self> {
        if fraction < 0.0 {
            return Err(PyValueError::new_err(format!(
                "Can not sample table with negative fraction: {fraction}"
            )));
        }
        if fraction > 1.0 {
            return Err(PyValueError::new_err(format!(
                "Can not sample table with fraction greater than 1.0: {fraction}"
            )));
        }
        py.detach(|| {
            Ok(self
                .record_batch
                .sample_by_fraction(fraction, with_replacement, seed)?
                .into())
        })
    }

    #[pyo3(signature = (size, with_replacement, seed=None))]
    pub fn sample_by_size(
        &self,
        py: Python,
        size: i64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> PyResult<Self> {
        if size < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not sample table with negative size: {size}"
            )));
        }
        py.detach(|| {
            Ok(self
                .record_batch
                .sample(size as usize, with_replacement, seed)?
                .into())
        })
    }

    pub fn quantiles(&self, py: Python, num: i64) -> PyResult<Self> {
        if num < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not fetch quantile from table with negative number: {num}"
            )));
        }
        let num = num as usize;
        py.detach(|| Ok(self.record_batch.quantiles(num)?.into()))
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
        let exprs = BoundExpr::bind_all(&exprs, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .partition_by_hash(exprs.as_slice(), num_partitions as usize)?
                .into_iter()
                .map(std::convert::Into::into)
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
        py.detach(|| {
            Ok(self
                .record_batch
                .partition_by_random(num_partitions as usize, seed as u64)?
                .into_iter()
                .map(std::convert::Into::into)
                .collect::<Vec<Self>>())
        })
    }

    pub fn partition_by_range(
        &self,
        py: Python,
        partition_keys: Vec<PyExpr>,
        boundaries: &Self,
        descending: Vec<bool>,
    ) -> PyResult<Vec<Self>> {
        let exprs = BoundExpr::bind_all(&partition_keys, &self.record_batch.schema)?;
        py.detach(|| {
            Ok(self
                .record_batch
                .partition_by_range(
                    exprs.as_slice(),
                    &boundaries.record_batch,
                    descending.as_slice(),
                )?
                .into_iter()
                .map(std::convert::Into::into)
                .collect::<Vec<Self>>())
        })
    }

    pub fn partition_by_value(
        &self,
        py: Python,
        partition_keys: Vec<PyExpr>,
    ) -> PyResult<(Vec<Self>, Self)> {
        let exprs = BoundExpr::bind_all(&partition_keys, &self.record_batch.schema)?;
        py.detach(|| {
            let (tables, values) = self.record_batch.partition_by_value(exprs.as_slice())?;
            let pyrecordbatches = tables
                .into_iter()
                .map(std::convert::Into::into)
                .collect::<Vec<Self>>();
            let values = values.into();
            Ok((pyrecordbatches, values))
        })
    }

    pub fn add_monotonically_increasing_id(
        &self,
        py: Python,
        partition_num: u64,
        column_name: &str,
    ) -> PyResult<Self> {
        py.detach(|| {
            Ok(self
                .record_batch
                .add_monotonically_increasing_id(partition_num, 0, column_name)?
                .into())
        })
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.record_batch.len())
    }

    pub fn size_bytes(&self) -> usize {
        self.record_batch.size_bytes()
    }

    pub fn get_column(&self, idx: usize) -> PySeries {
        self.record_batch.get_column(idx).clone().into()
    }

    pub fn columns(&self) -> Vec<PySeries> {
        self.record_batch
            .columns()
            .iter()
            .cloned()
            .map(Into::into)
            .collect()
    }

    #[staticmethod]
    pub fn concat(py: Python, tables: Vec<Self>) -> PyResult<Self> {
        let record_batches: Vec<_> = tables.iter().map(|t| &t.record_batch).collect();
        py.detach(|| Ok(RecordBatch::concat(record_batches.as_slice())?.into()))
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
        Ok(self
            .record_batch
            .slice(start as usize, end as usize)?
            .into())
    }

    #[staticmethod]
    pub fn from_arrow_record_batches(
        py: Python,
        record_batches: Vec<Bound<PyAny>>,
        schema: &PySchema,
    ) -> PyResult<Self> {
        let record_batch =
            ffi::record_batch_from_arrow(py, record_batches.as_slice(), schema.schema.clone())?;
        Ok(Self { record_batch })
    }

    #[staticmethod]
    pub fn from_pylist_series(dict: IndexMap<String, PySeries>) -> PyResult<Self> {
        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<Series> = Vec::new();
        fields.reserve(dict.len());
        columns.reserve(dict.len());

        for (name, series) in dict {
            let series = series.series;
            fields.push(Field::new(name.as_str(), series.data_type().clone()));
            columns.push(series.rename(name));
        }

        let num_rows = columns.first().map_or(0, daft_core::series::Series::len);
        if !columns.is_empty() {
            let first = columns.first().unwrap();
            for s in columns.iter().skip(1) {
                if s.len() != first.len() {
                    return Err(DaftError::ValueError(format!(
                        "Mismatch in Series lengths when making a Table, {} vs {}",
                        s.len(),
                        first.len()
                    ))
                    .into());
                }
            }
        }

        Ok(Self {
            record_batch: RecordBatch::new_with_broadcast(Schema::new(fields), columns, num_rows)?,
        })
    }
    #[staticmethod]
    pub fn from_pyseries_list(pycolumns: Vec<PySeries>) -> PyResult<Self> {
        if pycolumns.is_empty() {
            return Ok(Self {
                record_batch: RecordBatch::empty(None),
            });
        }

        let first = pycolumns.first().unwrap().series.len();

        let num_rows = pycolumns.iter().map(|c| c.series.len()).max().unwrap();

        let (fields, columns) = pycolumns
            .into_iter()
            .map(|s| (s.series.field().clone(), s.series))
            .unzip::<_, _, Vec<Field>, Vec<Series>>();

        if first == 0 {
            return Ok(Self {
                record_batch: RecordBatch::empty(Some(Arc::new(Schema::new(fields)))),
            });
        }

        Ok(Self {
            record_batch: RecordBatch::new_with_broadcast(Schema::new(fields), columns, num_rows)?,
        })
    }

    pub fn to_arrow_record_batch(&self) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        Python::attach(|py| {
            let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
            ffi::record_batch_to_arrow(py, &self.record_batch, pyarrow)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (schema=None))]
    pub fn empty(schema: Option<PySchema>) -> Self {
        RecordBatch::empty(match schema {
            Some(s) => Some(s.schema),
            None => None,
        })
        .into()
    }

    pub fn to_file_infos(&self) -> PyResult<FileInfos> {
        let file_infos: FileInfos = self.record_batch.clone().try_into()?;
        Ok(file_infos)
    }

    #[staticmethod]
    pub fn from_file_infos(file_infos: &FileInfos) -> PyResult<Self> {
        let table: RecordBatch = file_infos.try_into()?;
        Ok(table.into())
    }

    #[staticmethod]
    pub fn from_ipc_stream(bytes: Bound<'_, PyBytes>, py: Python) -> PyResult<Self> {
        let buffer = bytes.as_bytes();
        let record_batch = py.detach(|| RecordBatch::from_ipc_stream(buffer))?;
        Ok(record_batch.into())
    }

    pub fn to_ipc_stream<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        let buffer = py.detach(|| self.record_batch.to_ipc_stream())?;
        let bytes = PyBytes::new(py, &buffer);
        Ok(bytes)
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(value: RecordBatch) -> Self {
        Self {
            record_batch: value,
        }
    }
}

impl From<PyRecordBatch> for RecordBatch {
    fn from(item: PyRecordBatch) -> Self {
        item.record_batch
    }
}

impl AsRef<RecordBatch> for PyRecordBatch {
    fn as_ref(&self) -> &RecordBatch {
        &self.record_batch
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyRecordBatch>()?;
    parent.add_class::<FileInfos>()?;
    parent.add_class::<FileInfo>()?;

    Ok(())
}
