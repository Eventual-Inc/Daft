use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_partitioning::{Partition, PartitionId, PartitionSet};
use daft_core::{
    join::JoinSide,
    prelude::*,
    python::{PySchema, PySeries, PyTimeUnit},
};
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::{
    Expr,
    expr::bound_expr::{BoundAggExpr, BoundExpr},
    python::PyExpr,
};
use daft_io::{IOStatsContext, python::IOConfig};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_recordbatch::{RecordBatch, python::PyRecordBatch};
use daft_scan::{DataSource, ScanTaskRef, storage_config::StorageConfig};
use daft_stats::{TableMetadata, TableStatistics};
use pyo3::{PyTypeInfo, exceptions::PyValueError, prelude::*, types::PyBytes};
use snafu::ResultExt;

use crate::{
    DaftCoreComputeSnafu, PyIOSnafu, micropartition::MicroPartition,
    partitioning::MicroPartitionSet,
};

#[pyclass(module = "daft.daft", frozen)]
#[derive(Clone, Debug)]
pub struct PyMicroPartition {
    pub inner: Arc<MicroPartition>,
}

#[pymethods]
impl PyMicroPartition {
    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(PySchema {
            schema: self.inner.schema(),
        })
    }

    pub fn column_names(&self) -> PyResult<Vec<String>> {
        Ok(self.inner.column_names())
    }

    #[deprecated(since = "TBD", note = "name-referenced columns")]
    pub fn get_column_by_name(&self, name: &str, py: Python) -> PyResult<PySeries> {
        let index = self.inner.schema().get_index(name)?;

        let tables = py.detach(|| self.inner.concat_or_get())?;
        match tables {
            None => Ok(Series::empty(name, &self.inner.schema.get_field(name)?.dtype).into()),
            Some(t) => Ok(t.get_column(index).clone().into()),
        }
    }

    pub fn get_column(&self, idx: usize, py: Python) -> PyResult<PySeries> {
        let tables = py.detach(|| self.inner.concat_or_get())?;

        match tables {
            None => {
                let field = &self.inner.schema()[idx];
                Ok(Series::empty(&field.name, &field.dtype).into())
            }
            Some(t) => Ok(t.get_column(idx).clone().into()),
        }
    }

    pub fn columns(&self, py: Python) -> PyResult<Vec<PySeries>> {
        let tables = py.detach(|| self.inner.concat_or_get())?;

        match tables {
            None => {
                let series = self
                    .inner
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| Series::empty(&f.name, &f.dtype).into())
                    .collect::<Vec<_>>();
                Ok(series)
            }
            Some(t) => Ok(t.columns().iter().map(|s| s.clone().into()).collect()),
        }
    }

    pub fn get_record_batches(&self, py: Python) -> Vec<PyRecordBatch> {
        let record_batches = py.detach(|| self.inner.record_batches());
        record_batches
            .iter()
            .map(|rb| PyRecordBatch {
                record_batch: rb.clone(),
            })
            .collect()
    }

    pub fn size_bytes(&self) -> usize {
        self.inner.size_bytes()
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.inner.len())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }

    pub fn __repr_html__(&self) -> PyResult<String> {
        todo!("[MICROPARTITION_INT] __repr_html__")
    }

    #[staticmethod]
    pub fn from_record_batches(record_batches: Vec<PyRecordBatch>) -> PyResult<Self> {
        match &record_batches[..] {
            [] => Ok(MicroPartition::empty(None).into()),
            [first, ..] => {
                let record_batches = Arc::new(
                    record_batches
                        .iter()
                        .map(|t| t.record_batch.clone())
                        .collect::<Vec<_>>(),
                );
                Ok(MicroPartition::new_loaded(
                    first.record_batch.schema.clone(),
                    record_batches,
                    // Don't compute statistics if data is already materialized
                    None,
                )
                .into())
            }
        }
    }

    #[staticmethod]
    #[pyo3(signature = (schema=None))]
    pub fn empty(schema: Option<PySchema>) -> PyResult<Self> {
        Ok(MicroPartition::empty(match schema {
            Some(s) => Some(s.schema),
            None => None,
        })
        .into())
    }

    #[staticmethod]
    pub fn from_arrow_record_batches(
        py: Python,
        record_batches: Vec<Bound<PyAny>>,
        schema: &PySchema,
    ) -> PyResult<Self> {
        // TODO: Cleanup and refactor code for sharing with Table
        let tables = record_batches
            .into_iter()
            .map(|rb| {
                daft_recordbatch::ffi::record_batch_from_arrow(py, &[rb], schema.schema.clone())
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(MicroPartition::new_loaded(schema.schema.clone(), Arc::new(tables), None).into())
    }

    // Export Methods
    pub fn to_record_batch(&self, py: Python) -> PyResult<PyRecordBatch> {
        let concatted = py.detach(|| self.inner.concat_or_get())?;
        match concatted {
            None => Ok(PyRecordBatch::empty(Some(self.schema()?))),
            Some(record_batch) => Ok(PyRecordBatch { record_batch }),
        }
    }

    // Compute Methods

    #[staticmethod]
    pub fn concat(py: Python, to_concat: Vec<Self>) -> PyResult<Self> {
        let mps_iter = to_concat.iter().map(|t| t.inner.as_ref());
        py.detach(|| Ok(MicroPartition::concat(mps_iter)?.into()))
    }

    #[staticmethod]
    pub fn concat_or_empty(py: Python, to_concat: Vec<Self>, schema: PySchema) -> PyResult<Self> {
        let mps_iter = to_concat.iter().map(|t| t.inner.as_ref());
        py.detach(|| Ok(MicroPartition::concat_or_empty(mps_iter, schema.schema)?.into()))
    }

    pub fn slice(&self, py: Python, start: i64, end: i64) -> PyResult<Self> {
        py.detach(|| Ok(self.inner.slice(start as usize, end as usize)?.into()))
    }

    pub fn cast_to_schema(&self, py: Python, schema: PySchema) -> PyResult<Self> {
        #[allow(deprecated)]
        py.detach(|| Ok(self.inner.cast_to_schema(schema.schema)?.into()))
    }

    pub fn eval_expression_list(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs = BoundExpr::bind_all(&exprs, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
                .eval_expression_list(converted_exprs.as_slice())?
                .into())
        })
    }

    pub fn take(&self, py: Python, idx: &PySeries) -> PyResult<Self> {
        py.detach(|| {
            let idx_arr = idx.series.cast(&DataType::UInt64)?;
            let taken = self.inner.take(idx_arr.u64()?)?;
            let mp = MicroPartition::new_loaded(
                taken.schema.clone(),
                Arc::new(vec![taken]),
                self.inner.statistics.clone(),
            );
            Ok(mp.into())
        })
    }

    pub fn filter(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs = BoundExpr::bind_all(&exprs, &self.inner.schema)?;
        py.detach(|| Ok(self.inner.filter(converted_exprs.as_slice())?.into()))
    }

    pub fn sort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> PyResult<Self> {
        let converted_exprs = BoundExpr::bind_all(&sort_keys, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
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
        let converted_exprs = BoundExpr::bind_all(&sort_keys, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
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
        let converted_to_agg: Vec<_> = BoundExpr::bind_all(&to_agg, &self.inner.schema)?
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
        let converted_group_by: Vec<_> = BoundExpr::bind_all(&group_by, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
                .agg(converted_to_agg.as_slice(), converted_group_by.as_slice())?
                .into())
        })
    }

    pub fn dedup(&self, py: Python, columns: Vec<PyExpr>) -> PyResult<Self> {
        let converted_columns = BoundExpr::bind_all(&columns, &self.inner.schema)?;
        py.detach(|| Ok(self.inner.dedup(converted_columns.as_slice())?.into()))
    }

    pub fn pivot(
        &self,
        py: Python,
        group_by: Vec<PyExpr>,
        pivot_col: PyExpr,
        values_col: PyExpr,
        names: Vec<String>,
    ) -> PyResult<Self> {
        let converted_group_by = BoundExpr::bind_all(&group_by, &self.inner.schema)?;
        let converted_pivot_col = BoundExpr::try_new(pivot_col, &self.inner.schema)?;
        let converted_values_col = BoundExpr::try_new(values_col, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
                .pivot(
                    converted_group_by.as_slice(),
                    converted_pivot_col,
                    converted_values_col,
                    names,
                )?
                .into())
        })
    }

    #[pyo3(signature = (
        right,
        left_on,
        right_on,
        how,
        null_equals_nulls=None
    ))]
    pub fn hash_join(
        &self,
        py: Python,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        how: JoinType,
        null_equals_nulls: Option<Vec<bool>>,
    ) -> PyResult<Self> {
        let left_exprs = BoundExpr::bind_all(&left_on, &self.inner.schema)?;
        let right_exprs = BoundExpr::bind_all(&right_on, &right.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
                .hash_join(
                    &right.inner,
                    left_exprs.as_slice(),
                    right_exprs.as_slice(),
                    null_equals_nulls,
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
        let left_exprs = BoundExpr::bind_all(&left_on, &self.inner.schema)?;
        let right_exprs = BoundExpr::bind_all(&right_on, &right.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
                .sort_merge_join(
                    &right.inner,
                    left_exprs.as_slice(),
                    right_exprs.as_slice(),
                    JoinType::Inner, // TODO: Expose other join types
                    is_sorted,
                )?
                .into())
        })
    }

    pub fn cross_join(
        &self,
        py: Python,
        right: &Self,
        outer_loop_side: JoinSide,
    ) -> PyResult<Self> {
        py.detach(|| Ok(self.inner.cross_join(&right.inner, outer_loop_side)?.into()))
    }

    pub fn explode(&self, py: Python, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_explode = BoundExpr::bind_all(&to_explode, &self.inner.schema)?;

        py.detach(|| Ok(self.inner.explode(converted_to_explode.as_slice())?.into()))
    }

    pub fn unpivot(
        &self,
        py: Python,
        ids: Vec<PyExpr>,
        values: Vec<PyExpr>,
        variable_name: &str,
        value_name: &str,
    ) -> PyResult<Self> {
        let converted_ids = BoundExpr::bind_all(&ids, &self.inner.schema)?;
        let converted_values = BoundExpr::bind_all(&values, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
                .unpivot(
                    converted_ids.as_slice(),
                    converted_values.as_slice(),
                    variable_name,
                    value_name,
                )?
                .into())
        })
    }

    pub fn head(&self, py: Python, num: i64) -> PyResult<Self> {
        py.detach(|| {
            if num < 0 {
                return Err(PyValueError::new_err(format!(
                    "Can not head MicroPartition with negative number: {num}"
                )));
            }
            Ok(self.inner.head(num as usize)?.into())
        })
    }

    #[pyo3(signature = (fraction, with_replacement, seed=None))]
    pub fn sample_by_fraction(
        &self,
        py: Python,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> PyResult<Self> {
        py.detach(|| {
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
            Ok(self
                .inner
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
        py.detach(|| {
            if size < 0 {
                return Err(PyValueError::new_err(format!(
                    "Can not sample table with negative size: {size}"
                )));
            }
            Ok(self
                .inner
                .sample_by_size(size as usize, with_replacement, seed)?
                .into())
        })
    }

    pub fn quantiles(&self, py: Python, num: i64) -> PyResult<Self> {
        py.detach(|| {
            if num < 0 {
                return Err(PyValueError::new_err(format!(
                    "Can not fetch quantile from table with negative number: {num}"
                )));
            }
            Ok(self.inner.quantiles(num as usize)?.into())
        })
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
        let exprs = BoundExpr::bind_all(&exprs, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
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
                .inner
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
        boundaries: &PyRecordBatch,
        descending: Vec<bool>,
    ) -> PyResult<Vec<Self>> {
        let exprs = BoundExpr::bind_all(&partition_keys, &self.inner.schema)?;
        py.detach(|| {
            Ok(self
                .inner
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
        let exprs = BoundExpr::bind_all(&partition_keys, &self.inner.schema)?;
        py.detach(|| {
            let (mps, values) = self.inner.partition_by_value(exprs.as_slice())?;
            let mps = mps
                .into_iter()
                .map(std::convert::Into::into)
                .collect::<Vec<Self>>();
            let values = values.into();
            Ok((mps, values))
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
                .inner
                .add_monotonically_increasing_id(partition_num, column_name)?
                .into())
        })
    }

    #[staticmethod]
    #[pyo3(signature = (
        uri,
        schema,
        storage_config,
        include_columns=None,
        num_rows=None
    ))]
    pub fn read_json(
        py: Python,
        uri: &str,
        schema: PySchema,
        storage_config: StorageConfig,
        include_columns: Option<Vec<String>>,
        num_rows: Option<usize>,
    ) -> PyResult<Self> {
        let py_table = read_json_into_py_table(
            py,
            uri,
            schema.clone(),
            storage_config,
            include_columns,
            num_rows,
        )?;
        let mp = crate::micropartition::MicroPartition::new_loaded(
            schema.into(),
            Arc::new(vec![py_table.into()]),
            None,
        );
        Ok(mp.into())
    }

    #[staticmethod]
    #[pyo3(signature = (
        uri,
        convert_options=None,
        parse_options=None,
        read_options=None,
        io_config=None,
        multithreaded_io=None
    ))]
    pub fn read_json_native(
        py: Python,
        uri: &str,
        convert_options: Option<JsonConvertOptions>,
        parse_options: Option<JsonParseOptions>,
        read_options: Option<JsonReadOptions>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<Self> {
        let mp = py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_json: for uri {uri}"));
            let io_config = io_config.unwrap_or_default().config.into();

            crate::micropartition::read_json_into_micropartition(
                [uri].as_ref(),
                convert_options,
                parse_options,
                read_options,
                io_config,
                multithreaded_io.unwrap_or(true),
                Some(io_stats),
            )
        })?;
        Ok(mp.into())
    }

    #[staticmethod]
    #[pyo3(signature = (
        uri,
        convert_options=None,
        parse_options=None,
        read_options=None,
        io_config=None,
        multithreaded_io=None
    ))]
    pub fn read_csv(
        py: Python,
        uri: &str,
        convert_options: Option<CsvConvertOptions>,
        parse_options: Option<CsvParseOptions>,
        read_options: Option<CsvReadOptions>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<Self> {
        let mp = py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_csv: for uri {uri}"));
            let io_config = io_config.unwrap_or_default().config.into();
            crate::micropartition::read_csv_into_micropartition(
                [uri].as_ref(),
                convert_options,
                parse_options,
                read_options,
                io_config,
                multithreaded_io.unwrap_or(true),
                Some(io_stats),
            )
        })?;
        Ok(mp.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    #[pyo3(signature = (
        uri,
        columns=None,
        start_offset=None,
        num_rows=None,
        row_groups=None,
        predicate=None,
        io_config=None,
        multithreaded_io=None,
        coerce_int96_timestamp_unit=None
    ))]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<String>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        predicate: Option<PyExpr>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Self> {
        let mp = py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uri}"));

            let io_config = io_config.unwrap_or_default().config.into();
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            crate::micropartition::read_parquet_into_micropartition(
                [uri].as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                None,
                row_groups.map(|rg| vec![Some(rg)]),
                predicate.map(|e| e.expr),
                None,
                io_config,
                Some(io_stats),
                1,
                multithreaded_io.unwrap_or(true),
                &schema_infer_options,
                None,
                None,
                None,
                None,
                None,
            )
        })?;
        Ok(mp.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    #[pyo3(signature = (
        uris,
        columns=None,
        start_offset=None,
        num_rows=None,
        row_groups=None,
        predicate=None,
        io_config=None,
        num_parallel_tasks=None,
        multithreaded_io=None,
        coerce_int96_timestamp_unit=None,
        chunk_size=None
    ))]
    pub fn read_parquet_bulk(
        py: Python,
        uris: Vec<String>,
        columns: Option<Vec<String>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<Option<Vec<i64>>>>,
        predicate: Option<PyExpr>,
        io_config: Option<IOConfig>,
        num_parallel_tasks: Option<i64>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
        chunk_size: Option<usize>,
    ) -> PyResult<Self> {
        let mp = py.detach(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uris:?}"));

            let io_config = io_config.unwrap_or_default().config.into();
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            crate::micropartition::read_parquet_into_micropartition(
                uris.iter().map(AsRef::as_ref).collect::<Vec<_>>().as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                None,
                row_groups,
                predicate.map(|e| e.expr),
                None,
                io_config,
                Some(io_stats),
                num_parallel_tasks.unwrap_or(128) as usize,
                multithreaded_io.unwrap_or(true),
                &schema_infer_options,
                None,
                None,
                None,
                chunk_size,
                None,
            )
        })?;
        Ok(mp.into())
    }

    #[staticmethod]
    #[pyo3(signature = (
        uri,
        io_config=None,
        multithreaded_io=None
    ))]
    pub fn read_warc(
        py: Python,
        uri: &str,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("WARC-Record-ID", daft_core::prelude::DataType::Utf8),
            Field::new("WARC-Type", daft_core::prelude::DataType::Utf8),
            Field::new(
                "WARC-Date",
                daft_core::prelude::DataType::Timestamp(
                    TimeUnit::Nanoseconds,
                    Some("Etc/UTC".to_string()),
                ),
            ),
            Field::new("Content-Length", daft_core::prelude::DataType::Int64),
            Field::new(
                "WARC-Identified-Payload-Type",
                daft_core::prelude::DataType::Utf8,
            ),
            Field::new("warc_content", daft_core::prelude::DataType::Binary),
            Field::new("warc_headers", daft_core::prelude::DataType::Utf8),
        ]));
        let mp = py.detach(|| {
            crate::micropartition::read_warc_into_micropartition(
                &[uri],
                schema.into(),
                io_config.unwrap_or_default().config.into(),
                multithreaded_io.unwrap_or(true),
                None,
            )
        })?;
        Ok(mp.into())
    }

    #[staticmethod]
    pub fn _from_loaded_table_state(
        py: Python,
        schema_bytes: &[u8],
        table_objs: Vec<pyo3::Py<pyo3::PyAny>>,
        metadata_bytes: &[u8],
        statistics_bytes: &[u8],
    ) -> PyResult<Self> {
        let config = bincode::config::legacy();
        let schema: Schema = bincode::serde::decode_from_slice(schema_bytes, config)
            .unwrap()
            .0;
        let metadata: TableMetadata = bincode::serde::decode_from_slice(metadata_bytes, config)
            .unwrap()
            .0;
        let statistics: Option<TableStatistics> =
            bincode::serde::decode_from_slice(statistics_bytes, config)
                .unwrap()
                .0;

        let tables = table_objs
            .into_iter()
            .map(|p| {
                Ok(p.getattr(py, pyo3::intern!(py, "_recordbatch"))?
                    .extract::<PyRecordBatch>(py)?
                    .record_batch)
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(MicroPartition {
            schema: Arc::new(schema),
            chunks: Arc::new(tables),
            metadata,
            statistics,
        }
        .into())
    }

    pub fn __reduce__(
        &self,
        py: Python,
    ) -> PyResult<(pyo3::Py<pyo3::PyAny>, pyo3::Py<pyo3::PyAny>)> {
        let config = bincode::config::legacy();
        let schema_bytes = PyBytes::new(
            py,
            &bincode::serde::encode_to_vec(&self.inner.schema, config).unwrap(),
        );

        let py_metadata_bytes = PyBytes::new(
            py,
            &bincode::serde::encode_to_vec(&self.inner.metadata, config).unwrap(),
        );
        let py_stats_bytes = PyBytes::new(
            py,
            &bincode::serde::encode_to_vec(&self.inner.statistics, config).unwrap(),
        );

        let tables = self.inner.record_batches();
        let _from_pytable = py
            .import(pyo3::intern!(py, "daft.recordbatch"))?
            .getattr(pyo3::intern!(py, "RecordBatch"))?
            .getattr(pyo3::intern!(py, "_from_pyrecordbatch"))?;

        let pytables = tables.iter().map(|t| PyRecordBatch {
            record_batch: t.clone(),
        });
        let pyobjs = pytables
            .map(|pt| _from_pytable.call1((pt,)))
            .collect::<PyResult<Vec<_>>>()?;
        Ok((
            Self::type_object(py)
                .getattr(pyo3::intern!(py, "_from_loaded_table_state"))?
                .into(),
            (schema_bytes, pyobjs, py_metadata_bytes, py_stats_bytes)
                .into_pyobject(py)?
                .into(),
        ))
    }

    pub fn write_to_ipc_stream<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        let buffer = py.detach(|| self.inner.write_to_ipc_stream())?;
        let bytes = PyBytes::new(py, &buffer);
        Ok(bytes)
    }

    #[staticmethod]
    pub fn read_from_ipc_stream(bytes: Bound<'_, PyBytes>, py: Python) -> PyResult<Self> {
        let buffer = bytes.as_bytes();
        let mp = py.detach(|| MicroPartition::read_from_ipc_stream(buffer))?;
        Ok(mp.into())
    }
}

pub fn read_json_into_py_table(
    py: Python,
    uri: &str,
    schema: PySchema,
    storage_config: StorageConfig,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyRecordBatch> {
    let read_options = py
        .import(pyo3::intern!(py, "daft.runners.partitioning"))?
        .getattr(pyo3::intern!(py, "TableReadOptions"))?
        .call1((num_rows, include_columns))?;
    let py_schema = py
        .import(pyo3::intern!(py, "daft.logical.schema"))?
        .getattr(pyo3::intern!(py, "Schema"))?
        .getattr(pyo3::intern!(py, "_from_pyschema"))?
        .call1((schema,))?;
    Ok(py
        .import(pyo3::intern!(py, "daft.recordbatch.recordbatch_io"))?
        .getattr(pyo3::intern!(py, "read_json"))?
        .call1((uri, py_schema, storage_config, read_options))?
        .getattr(pyo3::intern!(py, "to_record_batch"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_recordbatch"))?
        .extract()?)
}

#[allow(clippy::too_many_arguments)]
pub fn read_csv_into_py_table(
    py: Python,
    uri: &str,
    has_header: bool,
    delimiter: Option<char>,
    double_quote: bool,
    schema: PySchema,
    storage_config: StorageConfig,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyRecordBatch> {
    let py_schema = py
        .import(pyo3::intern!(py, "daft.logical.schema"))?
        .getattr(pyo3::intern!(py, "Schema"))?
        .getattr(pyo3::intern!(py, "_from_pyschema"))?
        .call1((schema,))?;
    let read_options = py
        .import(pyo3::intern!(py, "daft.runners.partitioning"))?
        .getattr(pyo3::intern!(py, "TableReadOptions"))?
        .call1((num_rows, include_columns))?;
    let header_idx = if has_header { Some(0) } else { None };
    let parse_options = py
        .import(pyo3::intern!(py, "daft.runners.partitioning"))?
        .getattr(pyo3::intern!(py, "TableParseCSVOptions"))?
        .call1((delimiter, header_idx, double_quote))?;
    Ok(py
        .import(pyo3::intern!(py, "daft.recordbatch.recordbatch_io"))?
        .getattr(pyo3::intern!(py, "read_csv"))?
        .call1((uri, py_schema, storage_config, parse_options, read_options))?
        .getattr(pyo3::intern!(py, "to_record_batch"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_recordbatch"))?
        .extract()?)
}

pub fn read_parquet_into_py_table(
    py: Python,
    uri: &str,
    schema: PySchema,
    coerce_int96_timestamp_unit: PyTimeUnit,
    storage_config: StorageConfig,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyRecordBatch> {
    let py_schema = py
        .import(pyo3::intern!(py, "daft.logical.schema"))?
        .getattr(pyo3::intern!(py, "Schema"))?
        .getattr(pyo3::intern!(py, "_from_pyschema"))?
        .call1((schema,))?;
    let read_options = py
        .import(pyo3::intern!(py, "daft.runners.partitioning"))?
        .getattr(pyo3::intern!(py, "TableReadOptions"))?
        .call1((num_rows, include_columns))?;
    let py_coerce_int96_timestamp_unit = py
        .import(pyo3::intern!(py, "daft.datatype"))?
        .getattr(pyo3::intern!(py, "TimeUnit"))?
        .getattr(pyo3::intern!(py, "_from_pytimeunit"))?
        .call1((coerce_int96_timestamp_unit,))?;
    let parse_options = py
        .import(pyo3::intern!(py, "daft.runners.partitioning"))?
        .getattr(pyo3::intern!(py, "TableParseParquetOptions"))?
        .call1((py_coerce_int96_timestamp_unit,))?;
    Ok(py
        .import(pyo3::intern!(py, "daft.recordbatch.recordbatch_io"))?
        .getattr(pyo3::intern!(py, "read_parquet"))?
        .call1((uri, py_schema, storage_config, read_options, parse_options))?
        .getattr(pyo3::intern!(py, "to_record_batch"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_recordbatch"))?
        .extract()?)
}

pub fn read_sql_into_py_table(
    py: Python,
    sql: &str,
    conn: &pyo3::Py<pyo3::PyAny>,
    predicate: Option<PyExpr>,
    schema: PySchema,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyRecordBatch> {
    let py_schema = py
        .import(pyo3::intern!(py, "daft.logical.schema"))?
        .getattr(pyo3::intern!(py, "Schema"))?
        .getattr(pyo3::intern!(py, "_from_pyschema"))?
        .call1((schema,))?;
    let py_predicate = match predicate {
        Some(p) => Some(
            py.import(pyo3::intern!(py, "daft.expressions.expressions"))?
                .getattr(pyo3::intern!(py, "Expression"))?
                .getattr(pyo3::intern!(py, "_from_pyexpr"))?
                .call1((p,))?,
        ),
        None => None,
    };
    let read_options = py
        .import(pyo3::intern!(py, "daft.runners.partitioning"))?
        .getattr(pyo3::intern!(py, "TableReadOptions"))?
        .call1((num_rows, include_columns))?;
    Ok(py
        .import(pyo3::intern!(py, "daft.recordbatch.recordbatch_io"))?
        .getattr(pyo3::intern!(py, "read_sql"))?
        .call1((sql, conn, py_schema, read_options, py_predicate))?
        .getattr(pyo3::intern!(py, "to_record_batch"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_recordbatch"))?
        .extract()?)
}

pub fn read_pyfunc_into_table_iter(
    scan_task: ScanTaskRef,
) -> crate::Result<impl Iterator<Item = crate::Result<RecordBatch>>> {
    let table_iterators = scan_task.sources.iter().map(|source| {
        // Call Python function to create an Iterator (Grabs the GIL and then releases it)
        match source {
            DataSource::PythonFactoryFunction {
                module,
                func_name,
                func_args,
                ..
            } => {
                Python::attach(|py| {
                    let func = py.import(module.as_str())
                        .unwrap_or_else(|_| panic!("Cannot import factory function from module {module}"))
                        .getattr(func_name.as_str())
                        .unwrap_or_else(|_| panic!("Cannot find function {func_name} in module {module}"));
                    func.call(func_args.to_pytuple(py).with_context(|_| PyIOSnafu)?, None)
                        .with_context(|_| PyIOSnafu)
                        .map(Into::<pyo3::Py<pyo3::PyAny>>::into)
                })
            },
            _ => unreachable!("PythonFunction file format must be paired with PythonFactoryFunction data file sources"),
        }
    }).collect::<crate::Result<Vec<_>>>()?;

    let scan_task_limit = scan_task.pushdowns.limit;
    // If aggregation pushdown is present, the Python factory function is expected to have applied
    // the filtering semantics already (e.g., filter+count pushdown), so we should not re-apply
    // post-scan filters here to avoid double filtering on pre-aggregated results.
    // This removes reliance on any hard-coded Python function names and makes the behavior generic
    // for all sources that surface aggregation pushdowns.
    let scan_task_filters = if scan_task.pushdowns.aggregation.is_some() {
        None
    } else {
        scan_task.pushdowns.filters.clone()
    };
    let res = table_iterators
        .into_iter()
        .flat_map(move |iter| {
            std::iter::from_fn(move || {
                Python::attach(|py| {
                    iter.cast_bound::<pyo3::types::PyIterator>(py)
                        .expect("Function must return an iterator of tables")
                        .clone()
                        .next()
                        .map(|result| {
                            result
                                .map(|tbl| {
                                    tbl.extract::<daft_recordbatch::python::PyRecordBatch>()
                                        .expect("Must be a PyRecordBatch")
                                        .record_batch
                                })
                                .with_context(|_| PyIOSnafu)
                        })
                })
            })
        })
        .scan(0, move |rows_seen_so_far, table| {
            if scan_task_limit
                .map(|limit| *rows_seen_so_far >= limit)
                .unwrap_or(false)
            {
                return None;
            }
            match table {
                Err(e) => Some(Err(e)),
                Ok(table) => {
                    // Apply filters
                    let post_pushdown_table = || -> crate::Result<RecordBatch> {
                        let table = if let Some(filters) = scan_task_filters.as_ref() {
                            let filters = BoundExpr::try_new(filters.clone(), &table.schema)
                                .with_context(|_| DaftCoreComputeSnafu)?;

                            table
                                .filter(&[filters])
                                .with_context(|_| DaftCoreComputeSnafu)?
                        } else {
                            table
                        };

                        // Apply limit if necessary, and update `&mut remaining`
                        if let Some(limit) = scan_task_limit {
                            let limited_table = if *rows_seen_so_far + table.len() > limit {
                                table
                                    .slice(0, limit - *rows_seen_so_far)
                                    .with_context(|_| DaftCoreComputeSnafu)?
                            } else {
                                table
                            };

                            // Update the rows_seen_so_far
                            *rows_seen_so_far += limited_table.len();

                            Ok(limited_table)
                        } else {
                            Ok(table)
                        }
                    }();

                    Some(post_pushdown_table)
                }
            }
        });

    Ok(res)
}

impl From<MicroPartition> for PyMicroPartition {
    fn from(value: MicroPartition) -> Self {
        Arc::new(value).into()
    }
}

impl From<Arc<MicroPartition>> for PyMicroPartition {
    fn from(value: Arc<MicroPartition>) -> Self {
        Self { inner: value }
    }
}

impl From<PyMicroPartition> for Arc<MicroPartition> {
    fn from(value: PyMicroPartition) -> Self {
        value.inner
    }
}

#[pyclass(frozen, module = "daft.daft")]
#[derive(Clone, Debug)]
pub struct PyMicroPartitionSet(Arc<MicroPartitionSet>);

#[pymethods]
impl PyMicroPartitionSet {
    #[new]
    fn new() -> Self {
        Self(Arc::new(MicroPartitionSet::empty()))
    }

    fn get_partition(&self, idx: PartitionId) -> PyResult<PyMicroPartition> {
        Ok(self.0.get_partition(&idx)?.into())
    }

    fn set_partition(&self, idx: PartitionId, part: PyMicroPartition) -> PyResult<()> {
        Ok(self.0.set_partition(idx, &part.inner)?)
    }

    fn delete_partition(&self, idx: PartitionId) -> PyResult<()> {
        Ok(self.0.delete_partition(&idx)?)
    }

    fn has_partition(&self, idx: PartitionId) -> PyResult<bool> {
        Ok(self.0.has_partition(&idx))
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(self.0.len())
    }

    fn size_bytes(&self) -> PyResult<usize> {
        Ok(self.0.size_bytes()?)
    }

    fn num_partitions(&self) -> PyResult<usize> {
        Ok(self.0.num_partitions())
    }

    fn wait(&self) -> PyResult<()> {
        Ok(())
    }

    fn get_merged_micropartition(&self) -> PyResult<PyMicroPartition> {
        Ok(self.0.get_merged_partitions()?.into())
    }

    fn get_preview_micropartitions(&self, num_rows: usize) -> PyResult<Vec<PyMicroPartition>> {
        Ok(self
            .0
            .get_preview_partitions(num_rows)?
            .into_iter()
            .map(|p| p.into())
            .collect())
    }

    fn items(&self) -> PyResult<Vec<(PartitionId, PyMicroPartition)>> {
        Ok(self
            .0
            .items()
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect())
    }
}

impl From<MicroPartitionSet> for PyMicroPartitionSet {
    fn from(value: MicroPartitionSet) -> Self {
        Arc::new(value).into()
    }
}

impl From<Arc<MicroPartitionSet>> for PyMicroPartitionSet {
    fn from(value: Arc<MicroPartitionSet>) -> Self {
        Self(value)
    }
}

impl From<PyMicroPartitionSet> for Arc<MicroPartitionSet> {
    fn from(value: PyMicroPartitionSet) -> Self {
        value.0
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyMicroPartition>()?;
    parent.add_class::<PyMicroPartitionSet>()?;
    Ok(())
}
