use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use daft_core::{
    join::JoinType,
    python::{datatype::PyTimeUnit, schema::PySchema, PySeries},
    schema::Schema,
    Series,
};
use daft_csv::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
use daft_dsl::python::PyExpr;
use daft_io::{python::IOConfig, IOStatsContext};
use daft_json::{JsonConvertOptions, JsonParseOptions, JsonReadOptions};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_scan::{python::pylib::PyScanTask, storage_config::PyStorageConfig, ScanTask};
use daft_stats::TableStatistics;
use daft_table::python::PyTable;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};

use crate::micropartition::{MicroPartition, TableState};

use daft_stats::TableMetadata;
use pyo3::PyTypeInfo;

#[pyclass(module = "daft.daft", frozen)]
#[derive(Clone)]
pub struct PyMicroPartition {
    inner: Arc<MicroPartition>,
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

    pub fn get_column(&self, name: &str, py: Python) -> PyResult<PySeries> {
        let tables = py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("PyMicroPartition::get_column: {name}"));
            self.inner.concat_or_get(io_stats)
        })?;
        let columns = tables
            .iter()
            .map(|t| t.get_column(name))
            .collect::<DaftResult<Vec<_>>>()?;
        match columns.as_slice() {
            [] => Ok(Series::empty(name, &self.inner.schema.get_field(name)?.dtype).into()),
            columns => Ok(Series::concat(columns)?.into()),
        }
    }

    pub fn size_bytes(&self) -> PyResult<Option<usize>> {
        Ok(self.inner.size_bytes()?)
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

    // Creation Methods
    #[staticmethod]
    pub fn from_scan_task(scan_task: PyScanTask, py: Python) -> PyResult<Self> {
        Ok(py
            .allow_threads(|| {
                let io_stats = IOStatsContext::new(format!(
                    "MicroPartition::from_scan_task for {:?}",
                    scan_task.0.sources
                ));
                MicroPartition::from_scan_task(scan_task.into(), io_stats)
            })?
            .into())
    }

    #[staticmethod]
    pub fn from_tables(tables: Vec<PyTable>) -> PyResult<Self> {
        match &tables[..] {
            [] => Ok(MicroPartition::empty(None).into()),
            [first, ..] => {
                let tables = Arc::new(tables.iter().map(|t| t.table.clone()).collect::<Vec<_>>());
                Ok(MicroPartition::new_loaded(
                    first.table.schema.clone(),
                    tables,
                    // Don't compute statistics if data is already materialized
                    None,
                )
                .into())
            }
        }
    }

    #[staticmethod]
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
        record_batches: Vec<&PyAny>,
        schema: &PySchema,
    ) -> PyResult<Self> {
        // TODO: Cleanup and refactor code for sharing with Table
        let tables = record_batches
            .iter()
            .map(|rb| daft_table::ffi::record_batches_to_table(py, &[rb], schema.schema.clone()))
            .collect::<PyResult<Vec<_>>>()?;

        Ok(MicroPartition::new_loaded(schema.schema.clone(), Arc::new(tables), None).into())
    }

    // Export Methods
    pub fn to_table(&self, py: Python) -> PyResult<PyTable> {
        let concatted = py.allow_threads(|| {
            let io_stats = IOStatsContext::new("PyMicroPartition::to_table");
            self.inner.concat_or_get(io_stats)
        })?;
        match &concatted.as_ref()[..] {
            [] => PyTable::empty(Some(self.schema()?)),
            [table] => Ok(PyTable {
                table: table.clone(),
            }),
            [..] => unreachable!("concat_or_get should return one or none"),
        }
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
        py.allow_threads(|| Ok(self.inner.cast_to_schema(schema.schema)?.into()))
    }

    pub fn eval_expression_list(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::ExprRef> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .eval_expression_list(converted_exprs.as_slice())?
                .into())
        })
    }

    pub fn take(&self, py: Python, idx: &PySeries) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.take(&idx.series)?.into()))
    }

    pub fn filter(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::ExprRef> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| Ok(self.inner.filter(converted_exprs.as_slice())?.into()))
    }

    pub fn sort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::ExprRef> =
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
        let converted_exprs: Vec<daft_dsl::ExprRef> =
            sort_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .argsort(converted_exprs.as_slice(), descending.as_slice())?
                .into())
        })
    }

    pub fn agg(&self, py: Python, to_agg: Vec<PyExpr>, group_by: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_agg: Vec<daft_dsl::ExprRef> =
            to_agg.into_iter().map(|e| e.into()).collect();
        let converted_group_by: Vec<daft_dsl::ExprRef> =
            group_by.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
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
        let converted_group_by: Vec<daft_dsl::ExprRef> =
            group_by.into_iter().map(|e| e.into()).collect();
        let converted_pivot_col: daft_dsl::ExprRef = pivot_col.into();
        let converted_values_col: daft_dsl::ExprRef = values_col.into();
        py.allow_threads(|| {
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

    pub fn hash_join(
        &self,
        py: Python,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
        how: JoinType,
    ) -> PyResult<Self> {
        let left_exprs: Vec<daft_dsl::ExprRef> = left_on.into_iter().map(|e| e.into()).collect();
        let right_exprs: Vec<daft_dsl::ExprRef> = right_on.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .hash_join(
                    &right.inner,
                    left_exprs.as_slice(),
                    right_exprs.as_slice(),
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
        let left_exprs: Vec<daft_dsl::ExprRef> = left_on.into_iter().map(|e| e.into()).collect();
        let right_exprs: Vec<daft_dsl::ExprRef> = right_on.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .sort_merge_join(
                    &right.inner,
                    left_exprs.as_slice(),
                    right_exprs.as_slice(),
                    is_sorted,
                )?
                .into())
        })
    }

    pub fn explode(&self, py: Python, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_explode: Vec<daft_dsl::ExprRef> =
            to_explode.into_iter().map(|e| e.expr).collect();

        py.allow_threads(|| Ok(self.inner.explode(converted_to_explode.as_slice())?.into()))
    }

    pub fn unpivot(
        &self,
        py: Python,
        ids: Vec<PyExpr>,
        values: Vec<PyExpr>,
        variable_name: &str,
        value_name: &str,
    ) -> PyResult<Self> {
        let converted_ids: Vec<daft_dsl::ExprRef> = ids.into_iter().map(|e| e.into()).collect();
        let converted_values: Vec<daft_dsl::ExprRef> =
            values.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
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
        py.allow_threads(|| {
            if num < 0 {
                return Err(PyValueError::new_err(format!(
                    "Can not head MicroPartition with negative number: {num}"
                )));
            }
            Ok(self.inner.head(num as usize)?.into())
        })
    }

    pub fn sample_by_fraction(
        &self,
        py: Python,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
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

    pub fn sample_by_size(
        &self,
        py: Python,
        size: i64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
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
        py.allow_threads(|| {
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
        let exprs: Vec<daft_dsl::ExprRef> = exprs.into_iter().map(|e| e.into()).collect();
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
        let exprs: Vec<daft_dsl::ExprRef> = partition_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .partition_by_range(exprs.as_slice(), &boundaries.table, descending.as_slice())?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<Self>>())
        })
    }

    pub fn partition_by_value(
        &self,
        py: Python,
        partition_keys: Vec<PyExpr>,
    ) -> PyResult<(Vec<Self>, Self)> {
        let exprs: Vec<daft_dsl::ExprRef> = partition_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            let (mps, values) = self.inner.partition_by_value(exprs.as_slice())?;
            let mps = mps.into_iter().map(|m| m.into()).collect::<Vec<Self>>();
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
        py.allow_threads(|| {
            Ok(self
                .inner
                .add_monotonically_increasing_id(partition_num, column_name)?
                .into())
        })
    }

    #[staticmethod]
    pub fn read_json(
        py: Python,
        uri: &str,
        schema: PySchema,
        storage_config: PyStorageConfig,
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
    pub fn read_json_native(
        py: Python,
        uri: &str,
        convert_options: Option<JsonConvertOptions>,
        parse_options: Option<JsonParseOptions>,
        read_options: Option<JsonReadOptions>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<Self> {
        let mp = py.allow_threads(|| {
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
    pub fn read_csv(
        py: Python,
        uri: &str,
        convert_options: Option<CsvConvertOptions>,
        parse_options: Option<CsvParseOptions>,
        read_options: Option<CsvReadOptions>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
    ) -> PyResult<Self> {
        let mp = py.allow_threads(|| {
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
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        predicate: Option<PyExpr>,
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
            )
        })?;
        Ok(mp.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    pub fn read_parquet_bulk(
        py: Python,
        uris: Vec<&str>,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<Option<Vec<i64>>>>,
        predicate: Option<PyExpr>,
        io_config: Option<IOConfig>,
        num_parallel_tasks: Option<i64>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Self> {
        let mp = py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uris:?}"));

            let io_config = io_config.unwrap_or_default().config.into();
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            crate::micropartition::read_parquet_into_micropartition(
                uris.as_ref(),
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
            )
        })?;
        Ok(mp.into())
    }

    #[staticmethod]
    pub fn _from_unloaded_table_state(
        schema_bytes: &PyBytes,
        loading_scan_task_bytes: &PyBytes,
        metadata_bytes: &PyBytes,
        statistics_bytes: &PyBytes,
    ) -> PyResult<Self> {
        let schema = bincode::deserialize::<Schema>(schema_bytes.as_bytes()).unwrap();
        let scan_task =
            bincode::deserialize::<ScanTask>(loading_scan_task_bytes.as_bytes()).unwrap();
        let metadata = bincode::deserialize::<TableMetadata>(metadata_bytes.as_bytes()).unwrap();
        let statistics =
            bincode::deserialize::<Option<TableStatistics>>(statistics_bytes.as_bytes()).unwrap();

        Ok(MicroPartition {
            schema: Arc::new(schema),
            state: Mutex::new(TableState::Unloaded(Arc::new(scan_task))),
            metadata,
            statistics,
        }
        .into())
    }

    #[staticmethod]
    pub fn _from_loaded_table_state(
        py: Python,
        schema_bytes: &PyBytes,
        table_objs: Vec<PyObject>,
        metadata_bytes: &PyBytes,
        statistics_bytes: &PyBytes,
    ) -> PyResult<Self> {
        let schema = bincode::deserialize::<Schema>(schema_bytes.as_bytes()).unwrap();
        let metadata = bincode::deserialize::<TableMetadata>(metadata_bytes.as_bytes()).unwrap();
        let statistics =
            bincode::deserialize::<Option<TableStatistics>>(statistics_bytes.as_bytes()).unwrap();

        let tables = table_objs
            .into_iter()
            .map(|p| {
                Ok(p.getattr(py, pyo3::intern!(py, "_table"))?
                    .extract::<PyTable>(py)?
                    .table)
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(MicroPartition {
            schema: Arc::new(schema),
            state: Mutex::new(TableState::Loaded(Arc::new(tables))),
            metadata,
            statistics,
        }
        .into())
    }

    pub fn __reduce__(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
        let schema_bytes = PyBytes::new(py, &bincode::serialize(&self.inner.schema).unwrap());

        let py_metadata_bytes =
            PyBytes::new(py, &bincode::serialize(&self.inner.metadata).unwrap());
        let py_stats_bytes = PyBytes::new(py, &bincode::serialize(&self.inner.statistics).unwrap());

        let guard = self.inner.state.lock().unwrap();
        if let TableState::Loaded(tables) = guard.deref() {
            let _from_pytable = py
                .import(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "Table"))?
                .getattr(pyo3::intern!(py, "_from_pytable"))?;

            let pytables = tables.iter().map(|t| PyTable { table: t.clone() });
            let pyobjs = pytables
                .map(|pt| _from_pytable.call1((pt,)))
                .collect::<PyResult<Vec<_>>>()?;
            Ok((
                Self::type_object(py)
                    .getattr(pyo3::intern!(py, "_from_loaded_table_state"))?
                    .to_object(py),
                (schema_bytes, pyobjs, py_metadata_bytes, py_stats_bytes).to_object(py),
            ))
        } else if let TableState::Unloaded(params) = guard.deref() {
            let py_params_bytes = PyBytes::new(py, &bincode::serialize(params).unwrap());
            Ok((
                Self::type_object(py)
                    .getattr(pyo3::intern!(py, "_from_unloaded_table_state"))?
                    .to_object(py),
                (
                    schema_bytes,
                    py_params_bytes,
                    py_metadata_bytes,
                    py_stats_bytes,
                )
                    .to_object(py),
            ))
        } else {
            unreachable!()
        }
    }
}

pub(crate) fn read_json_into_py_table(
    py: Python,
    uri: &str,
    schema: PySchema,
    storage_config: PyStorageConfig,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyTable> {
    let read_options = py
        .import(pyo3::intern!(py, "daft.runners.partitioning"))?
        .getattr(pyo3::intern!(py, "TableReadOptions"))?
        .call1((num_rows, include_columns))?;
    let py_schema = py
        .import(pyo3::intern!(py, "daft.logical.schema"))?
        .getattr(pyo3::intern!(py, "Schema"))?
        .getattr(pyo3::intern!(py, "_from_pyschema"))?
        .call1((schema,))?;
    py.import(pyo3::intern!(py, "daft.table.table_io"))?
        .getattr(pyo3::intern!(py, "read_json"))?
        .call1((uri, py_schema, storage_config, read_options))?
        .getattr(pyo3::intern!(py, "to_table"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_table"))?
        .extract()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn read_csv_into_py_table(
    py: Python,
    uri: &str,
    has_header: bool,
    delimiter: Option<char>,
    double_quote: bool,
    schema: PySchema,
    storage_config: PyStorageConfig,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyTable> {
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
    py.import(pyo3::intern!(py, "daft.table.table_io"))?
        .getattr(pyo3::intern!(py, "read_csv"))?
        .call1((uri, py_schema, storage_config, parse_options, read_options))?
        .getattr(pyo3::intern!(py, "to_table"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_table"))?
        .extract()
}

pub(crate) fn read_parquet_into_py_table(
    py: Python,
    uri: &str,
    schema: PySchema,
    coerce_int96_timestamp_unit: PyTimeUnit,
    storage_config: PyStorageConfig,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyTable> {
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
    py.import(pyo3::intern!(py, "daft.table.table_io"))?
        .getattr(pyo3::intern!(py, "read_parquet"))?
        .call1((uri, py_schema, storage_config, read_options, parse_options))?
        .getattr(pyo3::intern!(py, "to_table"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_table"))?
        .extract()
}

pub(crate) fn read_sql_into_py_table(
    py: Python,
    sql: &str,
    conn: &PyObject,
    predicate: Option<PyExpr>,
    schema: PySchema,
    include_columns: Option<Vec<String>>,
    num_rows: Option<usize>,
) -> PyResult<PyTable> {
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
    py.import(pyo3::intern!(py, "daft.table.table_io"))?
        .getattr(pyo3::intern!(py, "read_sql"))?
        .call1((sql, conn, py_schema, read_options, py_predicate))?
        .getattr(pyo3::intern!(py, "to_table"))?
        .call0()?
        .getattr(pyo3::intern!(py, "_table"))?
        .extract()
}

impl From<MicroPartition> for PyMicroPartition {
    fn from(value: MicroPartition) -> Self {
        Arc::new(value).into()
    }
}

impl From<Arc<MicroPartition>> for PyMicroPartition {
    fn from(value: Arc<MicroPartition>) -> Self {
        PyMicroPartition { inner: value }
    }
}

impl From<PyMicroPartition> for Arc<MicroPartition> {
    fn from(value: PyMicroPartition) -> Self {
        value.inner
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyMicroPartition>()?;
    Ok(())
}
