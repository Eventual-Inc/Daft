mod wrappers;

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_py_serde::{
    deserialize_py_object, impl_bincode_py_state_serialization, serialize_py_object,
};
use daft_recordbatch::{RecordBatch, python::PyRecordBatch};
use daft_schema::python::schema::PySchema;
use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
use pyo3::{prelude::*, types::PyTuple};
use serde::{Deserialize, Serialize};
pub use wrappers::{PyDataSourceTaskWrapper, PyDataSourceWrapper};

use crate::{
    CsvSourceConfig, DataSourceRef, DataSourceTaskRef, FileFormatConfig, JsonSourceConfig,
    ParquetSourceConfig, ScanSource, ScanSourceKind, ScanTask, SourceConfig, TextSourceConfig,
    WarcSourceConfig, source::ShimSourceTask, storage_config::StorageConfig,
};

/// A Rust [`DataSource`] exposed as a Python object.
#[pyclass(module = "daft.daft", name = "PyDataSource", from_py_object)]
#[derive(Clone)]
pub struct PyDataSource(DataSourceRef);

/// Convert a `DataSourceRef` to a `PyDataSource`.
impl From<DataSourceRef> for PyDataSource {
    fn from(source: DataSourceRef) -> Self {
        Self(source)
    }
}

/// Convert a `PyDataSource` to a `DataSourceRef`.
impl From<PyDataSource> for DataSourceRef {
    fn from(source: PyDataSource) -> Self {
        source.0
    }
}

#[pymethods]
impl PyDataSource {
    fn name(&self) -> String {
        self.0.name()
    }

    fn schema(&self) -> PySchema {
        PySchema {
            schema: self.0.schema(),
        }
    }
}

/// A Rust [`DataSourceTask`] exposed as a Python object.
#[pyclass(module = "daft.daft", name = "PyDataSourceTask", from_py_object)]
#[derive(Clone)]
pub struct PyDataSourceTask(DataSourceTaskRef);

#[pymethods]
impl PyDataSourceTask {
    fn schema(&self) -> PySchema {
        PySchema {
            schema: self.0.schema(),
        }
    }

    /// Create a task that reads a Parquet file using the native reader.
    ///
    /// This wraps a native `ScanTask` in a `ShimSourceTask` (which implements
    /// `DataSourceTask`), so it can be yielded from `DataSource.get_tasks()` and
    /// the `ScanOperator` bridge will unwrap it back to a `ScanTask` for execution.
    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    #[pyo3(signature = (
        path,
        schema,
        *,
        pushdowns = None,
        num_rows = None,
        size_bytes = None,
        partition_values = None,
        stats = None,
        storage_config = None,
    ))]
    fn parquet(
        path: String,
        schema: PySchema,
        pushdowns: Option<pylib_scan_info::PyPushdowns>,
        num_rows: Option<i64>,
        size_bytes: Option<u64>,
        partition_values: Option<PyRecordBatch>,
        stats: Option<PyRecordBatch>,
        storage_config: Option<StorageConfig>,
    ) -> PyResult<Self> {
        let storage_config = storage_config.unwrap_or_default().into();

        // Strip partition_filters — in the DataSource model, partition pruning
        // is the DataSource's job (it decides which files to yield).
        let pushdowns = pushdowns
            .map(|p| {
                let mut pd = p.0.as_ref().clone();
                pd.partition_filters = None;
                pd
            })
            .unwrap_or_default();

        let pspec = PartitionSpec {
            keys: partition_values.map_or_else(|| RecordBatch::empty(None), |p| p.record_batch),
        };
        let statistics = stats
            .map(|s| TableStatistics::from_stats_table(&s.record_batch))
            .transpose()?;

        let source = ScanSource {
            size_bytes,
            last_modified: None,
            metadata: num_rows.map(|n| TableMetadata { length: n as usize }),
            statistics,
            partition_spec: Some(pspec),
            kind: ScanSourceKind::File {
                path,
                chunk_spec: None,
                iceberg_delete_files: None,
                parquet_metadata: None,
            },
        };

        let scan_task = Arc::new(ScanTask::new(
            vec![source],
            Arc::new(SourceConfig::File(FileFormatConfig::Parquet(
                ParquetSourceConfig::default(),
            ))),
            schema.schema,
            storage_config,
            pushdowns,
            None,
        ));

        Ok(Self(Arc::new(ShimSourceTask::new(scan_task))))
    }
}

/// Configuration for parsing a particular file format.
#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "FileFormatConfig", from_py_object)
)]
pub struct PyFileFormatConfig(Arc<FileFormatConfig>);

#[pymethods]
impl PyFileFormatConfig {
    /// Create a Parquet file format config.
    #[staticmethod]
    fn from_parquet_config(config: ParquetSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Parquet(config)))
    }

    /// Create a CSV file format config.
    #[staticmethod]
    fn from_csv_config(config: CsvSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Csv(config)))
    }

    /// Create a JSON file format config.
    #[staticmethod]
    fn from_json_config(config: JsonSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Json(config)))
    }

    /// Create a Warc file format config.
    #[staticmethod]
    fn from_warc_config(config: WarcSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Warc(config)))
    }

    /// Create a TEXT file format config.
    #[staticmethod]
    fn from_text_config(config: TextSourceConfig) -> Self {
        Self(Arc::new(FileFormatConfig::Text(config)))
    }

    /// Get the underlying data source config.
    #[getter]
    fn get_config(&self, py: Python) -> PyResult<Py<PyAny>> {
        match self.0.as_ref() {
            FileFormatConfig::Parquet(config) => config
                .clone()
                .into_pyobject(py)
                .map(|c| c.unbind().into_any()),
            FileFormatConfig::Csv(config) => config
                .clone()
                .into_pyobject(py)
                .map(|c| c.unbind().into_any()),
            FileFormatConfig::Json(config) => config
                .clone()
                .into_pyobject(py)
                .map(|c| c.unbind().into_any()),
            FileFormatConfig::Warc(config) => config
                .clone()
                .into_pyobject(py)
                .map(|c| c.unbind().into_any()),
            FileFormatConfig::Text(config) => config
                .clone()
                .into_pyobject(py)
                .map(|c| c.unbind().into_any()),
        }
    }

    /// Get the file format for this file format config.
    fn file_format(&self) -> common_file_formats::FileFormat {
        self.0.as_ref().into()
    }

    fn __richcmp__(&self, other: &Self, op: pyo3::basic::CompareOp) -> bool {
        match op {
            pyo3::basic::CompareOp::Eq => self.0 == other.0,
            pyo3::basic::CompareOp::Ne => !self.__richcmp__(other, pyo3::basic::CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }
}

impl_bincode_py_state_serialization!(PyFileFormatConfig);

impl From<PyFileFormatConfig> for Arc<FileFormatConfig> {
    fn from(file_format_config: PyFileFormatConfig) -> Self {
        file_format_config.0
    }
}

impl From<Arc<FileFormatConfig>> for PyFileFormatConfig {
    fn from(file_format_config: Arc<FileFormatConfig>) -> Self {
        Self(file_format_config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PyObjectSerializableWrapper(
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub Arc<pyo3::Py<pyo3::PyAny>>,
);

/// Python arguments to a Python function that produces Tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonTablesFactoryArgs {
    args: Vec<PyObjectSerializableWrapper>,
    hash: u64,
}

impl Hash for PythonTablesFactoryArgs {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PythonTablesFactoryArgs {
    pub fn new(args: Vec<Arc<pyo3::Py<pyo3::PyAny>>>) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        Python::attach(|py| {
            for obj in &args {
                // Only hash hashable PyObjects.
                if let Ok(hash) = obj.bind(py).hash() {
                    hash.hash(&mut hasher);
                }
            }
        });
        Self {
            args: args.into_iter().map(PyObjectSerializableWrapper).collect(),
            hash: hasher.finish(),
        }
    }

    pub fn to_pytuple<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyTuple>> {
        pyo3::types::PyTuple::new(py, self.args.iter().map(|x| x.0.bind(py)))
    }
}

impl PartialEq for PythonTablesFactoryArgs {
    fn eq(&self, other: &Self) -> bool {
        if self.args.len() != other.args.len() {
            return false;
        }
        self.args
            .iter()
            .zip(other.args.iter())
            .all(|(s, o)| (s.0.as_ptr() as isize) == (o.0.as_ptr() as isize))
    }
}

pub mod pylib {
    use std::{default, sync::Arc};

    use common_daft_config::PyDaftExecutionConfig;
    use common_error::DaftResult;
    use common_py_serde::impl_bincode_py_state_serialization;
    use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr, python::PyExpr};
    use daft_recordbatch::{RecordBatch, python::PyRecordBatch};
    use daft_schema::{python::schema::PySchema, schema::SchemaRef};
    use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
    use pyo3::{prelude::*, pyclass, types::PyIterator};
    use serde::{Deserialize, Serialize};

    use super::{PyDataSourceWrapper, PyFileFormatConfig, PythonTablesFactoryArgs};
    use crate::{
        DatabaseSourceConfig, FileFormatConfig, PartitionField, Pushdowns, ScanOperator,
        ScanOperatorRef, ScanSource, ScanSourceKind, ScanTask, ScanTaskRef, SourceConfig,
        SupportsPushdownFilters,
        anonymous::AnonymousScanOperator,
        glob::GlobScanOperator,
        python::pylib_scan_info::{PyPartitionField, PyPushdowns},
        storage_config::StorageConfig,
    };

    #[pyclass(module = "daft.daft", frozen, from_py_object)]
    #[derive(Debug, Clone)]
    pub struct ScanOperatorHandle {
        scan_op: ScanOperatorRef,
    }

    #[pymethods]
    impl ScanOperatorHandle {
        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.scan_op))
        }

        #[staticmethod]
        pub fn anonymous_scan(
            py: Python,
            files: Vec<String>,
            schema: PySchema,
            file_format_config: PyFileFormatConfig,
            storage_config: StorageConfig,
        ) -> PyResult<Self> {
            py.detach(|| {
                let schema = schema.schema;
                let operator = Arc::new(AnonymousScanOperator::new(
                    files,
                    schema,
                    file_format_config.into(),
                    storage_config.into(),
                ));

                Ok(Self {
                    scan_op: ScanOperatorRef(operator),
                })
            })
        }

        #[staticmethod]
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (
            glob_path,
            file_format_config,
            storage_config,
            hive_partitioning,
            infer_schema,
            schema=None,
            file_path_column=None,
            skip_glob=false
        ))]
        pub fn glob_scan(
            py: Python,
            glob_path: Vec<String>,
            file_format_config: PyFileFormatConfig,
            storage_config: StorageConfig,
            hive_partitioning: bool,
            infer_schema: bool,
            schema: Option<PySchema>,
            file_path_column: Option<String>,
            skip_glob: bool,
        ) -> PyResult<Self> {
            py.detach(|| {
                let executor = common_runtime::get_io_runtime(true);

                let task = GlobScanOperator::try_new(
                    glob_path,
                    file_format_config.into(),
                    storage_config.into(),
                    infer_schema,
                    schema.map(|s| s.schema),
                    file_path_column,
                    hive_partitioning,
                    skip_glob,
                );

                let operator = executor.block_within_async_context(task)??;
                let operator = Arc::new(operator);

                Ok(Self {
                    scan_op: ScanOperatorRef(operator),
                })
            })
        }

        #[staticmethod]
        pub fn from_python_scan_operator(
            py_scan: pyo3::Py<pyo3::PyAny>,
            py: Python,
        ) -> PyResult<Self> {
            let scan_op = ScanOperatorRef(Arc::new(PythonScanOperatorBridge::from_python_abc(
                py_scan, py,
            )?));
            Ok(Self { scan_op })
        }

        /// Create a ScanOperatorHandle from a Python DataSource.
        ///
        /// Wraps the DataSource in a [`PyDataSourceWrapper`] which implements
        /// [`ScanOperator`] by draining the async task stream.
        #[staticmethod]
        pub fn from_data_source(source: Bound<'_, pyo3::PyAny>) -> Self {
            let wrapper = PyDataSourceWrapper::new(source);
            Self {
                scan_op: ScanOperatorRef(Arc::new(wrapper)),
            }
        }
    }

    #[pyclass(module = "daft.daft")]
    #[derive(Debug)]
    pub struct PythonScanOperatorBridge {
        name: String,
        operator: pyo3::Py<pyo3::PyAny>,
        schema: SchemaRef,
        partitioning_keys: Vec<PartitionField>,
        can_absorb_filter: bool,
        can_absorb_limit: bool,
        can_absorb_select: bool,
        supports_count_pushdown: bool,
        display_name: String,
    }

    impl PythonScanOperatorBridge {
        fn _name(abc: &pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<String> {
            let result = abc.call_method0(py, pyo3::intern!(py, "name"))?;
            result.extract::<String>(py)
        }
        fn _partitioning_keys(
            abc: &pyo3::Py<pyo3::PyAny>,
            py: Python,
        ) -> PyResult<Vec<PartitionField>> {
            let result = abc.call_method0(py, pyo3::intern!(py, "partitioning_keys"))?;
            result
                .bind(py)
                .try_iter()?
                .map(|p| Ok(p?.extract::<PyPartitionField>()?.0.as_ref().clone()))
                .collect()
        }

        fn _schema(abc: &pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<SchemaRef> {
            let python_schema = abc.call_method0(py, pyo3::intern!(py, "schema"))?;
            let pyschema = python_schema
                .getattr(py, pyo3::intern!(py, "_schema"))?
                .extract::<PySchema>(py)?;
            Ok(pyschema.schema)
        }

        fn _can_absorb_filter(abc: &pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<bool> {
            abc.call_method0(py, pyo3::intern!(py, "can_absorb_filter"))?
                .extract::<bool>(py)
        }

        fn _can_absorb_limit(abc: &pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<bool> {
            abc.call_method0(py, pyo3::intern!(py, "can_absorb_limit"))?
                .extract::<bool>(py)
        }

        fn _can_absorb_select(abc: &pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<bool> {
            abc.call_method0(py, pyo3::intern!(py, "can_absorb_select"))?
                .extract::<bool>(py)
        }

        fn _display_name(abc: &pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<String> {
            abc.call_method0(py, pyo3::intern!(py, "display_name"))?
                .extract::<String>(py)
        }

        fn _supports_count_pushdown(abc: &pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<bool> {
            abc.call_method0(py, pyo3::intern!(py, "supports_count_pushdown"))?
                .extract::<bool>(py)
        }
    }

    #[pymethods]
    impl PythonScanOperatorBridge {
        #[staticmethod]
        pub fn from_python_abc(abc: pyo3::Py<pyo3::PyAny>, py: Python) -> PyResult<Self> {
            let name = Self::_name(&abc, py)?;
            let partitioning_keys = Self::_partitioning_keys(&abc, py)?;
            let schema = Self::_schema(&abc, py)?;
            let can_absorb_filter = Self::_can_absorb_filter(&abc, py)?;
            let can_absorb_limit = Self::_can_absorb_limit(&abc, py)?;
            let can_absorb_select = Self::_can_absorb_select(&abc, py)?;
            let display_name = Self::_display_name(&abc, py)?;
            let supports_count_pushdown = Self::_supports_count_pushdown(&abc, py)?;

            Ok(Self {
                name,
                operator: abc,
                schema,
                partitioning_keys,
                can_absorb_filter,
                can_absorb_limit,
                can_absorb_select,
                display_name,
                supports_count_pushdown,
            })
        }
    }

    impl SupportsPushdownFilters for PythonScanOperatorBridge {
        fn push_filters(&self, filters: &[ExprRef]) -> (Vec<ExprRef>, Vec<ExprRef>) {
            Python::attach(|py| {
                let py_filters: Vec<pyo3::Py<pyo3::PyAny>> = filters
                    .iter()
                    .map(|expr| Py::new(py, PyExpr::from(expr.clone())).unwrap().into())
                    .collect();

                let result = match self
                    .operator
                    .call_method1(py, "push_filters", (py_filters,))
                {
                    Ok(res) => res,
                    Err(e) => {
                        e.print_and_set_sys_last_vars(py);
                        panic!(
                            "Python method call failed, please ensure return value is a tuple of (pushed_filters, post_filters)"
                        );
                    }
                };

                let (pushed_pyexpr, post_pyexpr): (Vec<PyExpr>, Vec<PyExpr>) = match result
                    .extract(py)
                {
                    Ok(res) => res,
                    Err(_) => {
                        let py_result = result.bind(py);

                        let elem1_type = py_result
                            .get_item(0)
                            .map(|e| {
                                let type_ = e.get_type();
                                let name = type_.name().unwrap();
                                name.to_string_lossy().into_owned()
                            })
                            .unwrap_or_else(|_| "unknown".to_string());

                        let elem2_type = py_result
                            .get_item(1)
                            .map(|e| {
                                let type_ = e.get_type();
                                let name = type_.name().unwrap();
                                name.to_string_lossy().into_owned()
                            })
                            .unwrap_or_else(|_| "unknown".to_string());

                        let full_type = py_result.get_type();
                        let type_name_obj = full_type.name().unwrap();
                        let type_name = type_name_obj.to_string_lossy();

                        panic!(
                            "Python return type mismatch. Expected tuple[list[Expr], list[Expr]]\nActual:\nElement 1 type: {}\nElement 2 type: {}\nFull type: {}",
                            elem1_type, elem2_type, type_name
                        );
                    }
                };

                (
                    pushed_pyexpr.into_iter().map(|e| e.expr).collect(),
                    post_pyexpr.into_iter().map(|e| e.expr).collect(),
                )
            })
        }
    }

    impl ScanOperator for PythonScanOperatorBridge {
        fn name(&self) -> &str {
            &self.name
        }
        fn partitioning_keys(&self) -> &[PartitionField] {
            &self.partitioning_keys
        }
        fn schema(&self) -> daft_schema::schema::SchemaRef {
            self.schema.clone()
        }
        fn file_path_column(&self) -> Option<&str> {
            None
        }
        fn generated_fields(&self) -> Option<SchemaRef> {
            None
        }
        fn can_absorb_filter(&self) -> bool {
            self.can_absorb_filter
        }
        fn can_absorb_limit(&self) -> bool {
            self.can_absorb_limit
        }
        fn can_absorb_select(&self) -> bool {
            self.can_absorb_select
        }

        fn can_absorb_shard(&self) -> bool {
            false
        }

        fn supports_count_pushdown(&self) -> bool {
            self.supports_count_pushdown
        }

        fn multiline_display(&self) -> Vec<String> {
            let lines = vec![format!("PythonScanOperator: {}", self.display_name)];
            lines
        }

        fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
            let scan_tasks = Python::attach(|py| {
                let pypd = PyPushdowns(pushdowns.clone().into()).into_pyobject(py)?;
                let pyiter =
                    self.operator
                        .call_method1(py, pyo3::intern!(py, "to_scan_tasks"), (pypd,))?;
                let pyiter = PyIterator::from_object(pyiter.bind(py))?;
                DaftResult::Ok(
                    pyiter
                        .map(|v| {
                            let pyscantask = v?.extract::<PyScanTask>()?.0;
                            DaftResult::Ok(pyscantask)
                        })
                        .collect::<Vec<_>>(),
                )
            })?;

            scan_tasks.into_iter().collect()
        }

        fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
            if self.can_absorb_filter {
                Some(self)
            } else {
                None
            }
        }
    }

    impl From<ScanOperatorRef> for ScanOperatorHandle {
        fn from(value: ScanOperatorRef) -> Self {
            Self { scan_op: value }
        }
    }

    impl From<ScanOperatorHandle> for ScanOperatorRef {
        fn from(value: ScanOperatorHandle) -> Self {
            value.scan_op
        }
    }

    #[pyclass(module = "daft.daft", name = "ScanTask", frozen, from_py_object)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyScanTask(pub Arc<ScanTask>);

    #[pymethods]
    impl PyScanTask {
        pub fn num_rows(&self) -> PyResult<Option<i64>> {
            Ok(self.0.num_rows().map(i64::try_from).transpose()?)
        }

        pub fn estimate_in_memory_size_bytes(
            &self,
            cfg: PyDaftExecutionConfig,
        ) -> PyResult<Option<i64>> {
            Ok(self
                .0
                .estimate_in_memory_size_bytes(Some(cfg.config.as_ref()))
                .map(i64::try_from)
                .transpose()?)
        }
    }

    #[pymethods]
    impl PyScanTask {
        #[allow(clippy::too_many_arguments)]
        #[staticmethod]
        #[pyo3(signature = (
            file,
            file_format,
            schema,
            storage_config,
            num_rows=None,
            size_bytes=None,
            iceberg_delete_files=None,
            pushdowns=None,
            partition_values=None,
            stats=None
        ))]
        pub fn catalog_scan_task(
            file: String,
            file_format: PyFileFormatConfig,
            schema: PySchema,
            storage_config: StorageConfig,
            num_rows: Option<i64>,
            size_bytes: Option<u64>,
            iceberg_delete_files: Option<Vec<String>>,
            pushdowns: Option<PyPushdowns>,
            partition_values: Option<PyRecordBatch>,
            stats: Option<PyRecordBatch>,
        ) -> PyResult<Option<Self>> {
            if let Some(ref pvalues) = partition_values
                && let Some(Some(partition_filters)) =
                    pushdowns.as_ref().map(|p| &p.0.partition_filters)
            {
                let table = &pvalues.record_batch;
                let partition_filters =
                    BoundExpr::try_new(partition_filters.clone(), &table.schema)?;
                let eval_pred = table.eval_expression_list(&[partition_filters])?;
                assert_eq!(eval_pred.num_columns(), 1);
                let series = eval_pred.get_column(0);
                assert_eq!(series.data_type(), &daft_core::datatypes::DataType::Boolean);
                let boolean = series.bool()?;
                assert_eq!(boolean.len(), 1);
                let value = boolean.get(0);
                match value {
                    None | Some(false) => return Ok(None),
                    Some(true) => {}
                }
            }
            // TODO(Clark): Filter out scan tasks with pushed down filters + table stats?

            let pspec = PartitionSpec {
                keys: partition_values.map_or_else(|| RecordBatch::empty(None), |p| p.record_batch),
            };
            let statistics = stats
                .map(|s| TableStatistics::from_stats_table(&s.record_batch))
                .transpose()?;

            let metadata = num_rows.map(|n| TableMetadata { length: n as usize });

            let data_source = ScanSource {
                size_bytes,
                last_modified: None,
                metadata,
                statistics,
                partition_spec: Some(pspec),
                kind: ScanSourceKind::File {
                    path: file,
                    chunk_spec: None,
                    iceberg_delete_files,
                    parquet_metadata: None,
                },
            };

            let file_format_config: Arc<FileFormatConfig> = file_format.into();
            let scan_task = ScanTask::new(
                vec![data_source],
                Arc::new(SourceConfig::File(Arc::unwrap_or_clone(file_format_config))),
                schema.schema,
                storage_config.into(),
                pushdowns.map(|p| p.0.as_ref().clone()).unwrap_or_default(),
                None,
            );
            Ok(Some(Self(Arc::new(scan_task))))
        }

        #[allow(clippy::too_many_arguments)]
        #[staticmethod]
        #[pyo3(signature = (
            url,
            config,
            schema,
            storage_config,
            num_rows=None,
            size_bytes=None,
            pushdowns=None,
            stats=None
        ))]
        pub fn sql_scan_task(
            url: String,
            config: DatabaseSourceConfig,
            schema: PySchema,
            storage_config: StorageConfig,
            num_rows: Option<i64>,
            size_bytes: Option<u64>,
            pushdowns: Option<PyPushdowns>,
            stats: Option<PyRecordBatch>,
        ) -> PyResult<Self> {
            let statistics = stats
                .map(|s| TableStatistics::from_stats_table(&s.record_batch))
                .transpose()?;
            let data_source = ScanSource {
                size_bytes,
                last_modified: None,
                metadata: num_rows.map(|n| TableMetadata { length: n as usize }),
                statistics,
                partition_spec: None,
                kind: ScanSourceKind::Database { path: url },
            };

            let scan_task = ScanTask::new(
                vec![data_source],
                Arc::new(SourceConfig::Database(config)),
                schema.schema,
                storage_config.into(),
                pushdowns.map(|p| p.0.as_ref().clone()).unwrap_or_default(),
                None,
            );
            Ok(Self(Arc::new(scan_task)))
        }

        #[allow(clippy::too_many_arguments)]
        #[staticmethod]
        #[pyo3(signature = (
            module,
            func_name,
            func_args,
            schema,
            num_rows=None,
            size_bytes=None,
            pushdowns=None,
            stats=None,
            source_name=None
        ))]
        pub fn python_factory_func_scan_task(
            module: String,
            func_name: String,
            func_args: Vec<pyo3::Py<pyo3::PyAny>>,
            schema: PySchema,
            num_rows: Option<i64>,
            size_bytes: Option<u64>,
            pushdowns: Option<PyPushdowns>,
            stats: Option<PyRecordBatch>,
            source_name: Option<String>,
        ) -> PyResult<Self> {
            let statistics = stats
                .map(|s| TableStatistics::from_stats_table(&s.record_batch))
                .transpose()?;
            let data_source = ScanSource {
                size_bytes,
                last_modified: None,
                metadata: num_rows.map(|num_rows| TableMetadata {
                    length: num_rows as usize,
                }),
                statistics,
                partition_spec: None,
                kind: ScanSourceKind::PythonFactoryFunction {
                    module: module.clone(),
                    func_name: func_name.clone(),
                    func_args: PythonTablesFactoryArgs::new(
                        func_args.into_iter().map(Arc::new).collect(),
                    ),
                },
            };

            let source_config = Arc::new(SourceConfig::PythonFunction {
                source_name,
                module_name: Some(module),
                function_name: Some(func_name),
            });

            let scan_task = ScanTask::new(
                vec![data_source],
                source_config,
                schema.schema,
                // HACK: StorageConfig isn't used when running the Python function but this is a non-optional arg for
                // ScanTask creation, so we just put in a placeholder here
                Arc::new(Default::default()),
                pushdowns.map(|p| p.0.as_ref().clone()).unwrap_or_default(),
                None,
            );
            Ok(Self(Arc::new(scan_task)))
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{:?}", self.0))
        }
    }

    impl From<Arc<ScanTask>> for PyScanTask {
        fn from(value: Arc<ScanTask>) -> Self {
            Self(value)
        }
    }

    impl From<PyScanTask> for Arc<ScanTask> {
        fn from(value: PyScanTask) -> Self {
            value.0
        }
    }

    impl_bincode_py_state_serialization!(PyScanTask);

    /// Estimates the in-memory size in bytes for a Parquet file.
    ///
    /// This function calculates an approximate size that the Parquet file would occupy
    /// when loaded into memory, considering only the specified columns if provided.
    ///
    /// Used for testing only.
    ///
    /// # Arguments
    ///
    /// * `uri` - A string slice that holds the URI of the Parquet file.
    /// * `file_size` - the size of the file on disk
    /// * `columns` - An optional vector of strings representing the column names to consider.
    ///               If None, all columns in the file will be considered.
    /// * `has_metadata` - whether or not metadata is pre-populated in the ScanTask. Defaults to false.
    ///
    /// # Returns
    ///
    /// Returns an `i64` representing the estimated size in bytes.
    ///
    #[pyfunction(signature = (uri, file_size, columns=None, has_metadata=None))]
    pub fn estimate_in_memory_size_bytes(
        uri: &str,
        file_size: u64,
        columns: Option<Vec<String>>,
        has_metadata: Option<bool>,
    ) -> PyResult<usize> {
        let io_runtime = common_runtime::get_io_runtime(true);
        let (schema, metadata) = io_runtime.block_on_current_thread(
            daft_parquet::read::read_parquet_schema_and_metadata(
                uri,
                default::Default::default(),
                None,
                default::Default::default(),
                None,
            ),
        )?;
        let data_source = ScanSource {
            size_bytes: Some(file_size),
            last_modified: None,
            metadata: if has_metadata.unwrap_or(false) {
                Some(TableMetadata {
                    length: metadata.num_rows(),
                })
            } else {
                None
            },
            statistics: None,
            partition_spec: None,
            kind: ScanSourceKind::File {
                path: uri.to_string(),
                chunk_spec: None,
                iceberg_delete_files: None,
                parquet_metadata: None,
            },
        };
        let st = ScanTask::new(
            vec![data_source],
            Arc::new(SourceConfig::File(FileFormatConfig::Parquet(
                default::Default::default(),
            ))),
            Arc::new(schema),
            Arc::new(Default::default()),
            Pushdowns::new(None, None, columns.map(Arc::new), None, None, None),
            None,
        );
        Ok(st.estimate_in_memory_size_bytes(None).unwrap())
    }
}

pub mod pylib_scan_info {
    use std::sync::Arc;

    use daft_core::count_mode::CountMode;
    use daft_dsl::{AggExpr, Expr, python::PyExpr};
    use daft_schema::python::field::PyField;
    use pyo3::{exceptions::PyAttributeError, prelude::*, pyclass};
    use serde::{Deserialize, Serialize};

    use crate::{PartitionField, PartitionTransform, Pushdowns};

    #[pyclass(
        module = "daft.daft",
        name = "PyPartitionField",
        frozen,
        from_py_object
    )]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPartitionField(pub Arc<PartitionField>);

    #[pymethods]
    impl PyPartitionField {
        #[new]
        #[pyo3(signature = (field, source_field=None, transform=None))]
        fn new(
            field: PyField,
            source_field: Option<PyField>,
            transform: Option<PyPartitionTransform>,
        ) -> PyResult<Self> {
            let p_field = PartitionField::new(
                field.field,
                source_field.map(std::convert::Into::into),
                transform.map(|e| e.0),
            )?;
            Ok(Self(Arc::new(p_field)))
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.0))
        }

        #[getter]
        pub fn field(&self) -> PyResult<PyField> {
            Ok(self.0.field.clone().into())
        }

        #[getter]
        pub fn source_field(&self) -> PyResult<Option<PyField>> {
            Ok(self.0.source_field.clone().map(Into::into))
        }

        #[getter]
        pub fn transform(&self) -> PyResult<Option<PyPartitionTransform>> {
            Ok(self.0.transform.map(PyPartitionTransform))
        }
    }

    #[pyclass(
        module = "daft.daft",
        name = "PyPartitionTransform",
        frozen,
        from_py_object
    )]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPartitionTransform(pub PartitionTransform);

    #[pymethods]
    impl PyPartitionTransform {
        #[staticmethod]
        pub fn identity() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Identity))
        }

        #[staticmethod]
        pub fn year() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Year))
        }

        #[staticmethod]
        pub fn month() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Month))
        }

        #[staticmethod]
        pub fn day() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Day))
        }

        #[staticmethod]
        pub fn hour() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Hour))
        }

        #[staticmethod]
        pub fn void() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Void))
        }

        #[staticmethod]
        pub fn iceberg_bucket(n: u64) -> PyResult<Self> {
            Ok(Self(PartitionTransform::IcebergBucket(n)))
        }

        #[staticmethod]
        pub fn iceberg_truncate(n: u64) -> PyResult<Self> {
            Ok(Self(PartitionTransform::IcebergTruncate(n)))
        }

        pub fn is_identity(&self) -> bool {
            matches!(self.0, PartitionTransform::Identity)
        }

        pub fn is_year(&self) -> bool {
            matches!(self.0, PartitionTransform::Year)
        }

        pub fn is_month(&self) -> bool {
            matches!(self.0, PartitionTransform::Month)
        }

        pub fn is_day(&self) -> bool {
            matches!(self.0, PartitionTransform::Day)
        }

        pub fn is_hour(&self) -> bool {
            matches!(self.0, PartitionTransform::Hour)
        }

        pub fn is_iceberg_bucket(&self) -> bool {
            matches!(self.0, PartitionTransform::IcebergBucket(_))
        }

        pub fn is_iceberg_truncate(&self) -> bool {
            matches!(self.0, PartitionTransform::IcebergTruncate(_))
        }

        pub fn num_buckets(&self) -> PyResult<u64> {
            match &self.0 {
                PartitionTransform::IcebergBucket(n) => Ok(*n),
                _ => Err(PyErr::new::<PyAttributeError, _>(
                    "Not an iceberg bucket transform",
                )),
            }
        }

        pub fn width(&self) -> PyResult<u64> {
            match &self.0 {
                PartitionTransform::IcebergTruncate(n) => Ok(*n),
                _ => Err(PyErr::new::<PyAttributeError, _>(
                    "Not an iceberg truncate transform",
                )),
            }
        }

        pub fn __eq__(&self, other: &Self) -> bool {
            self.0 == other.0
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.0))
        }
    }

    #[pyclass(module = "daft.daft", name = "PyPushdowns", frozen, from_py_object)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPushdowns(pub Arc<Pushdowns>);

    #[pymethods]
    impl PyPushdowns {
        #[new]
        #[pyo3(signature = (
            filters = None,
            partition_filters = None,
            columns = None,
            limit = None,
            aggregation = None,
        ))]
        pub fn new(
            filters: Option<PyExpr>,
            partition_filters: Option<PyExpr>,
            columns: Option<Vec<String>>,
            limit: Option<usize>,
            aggregation: Option<PyExpr>,
        ) -> Self {
            let pushdowns = Pushdowns::new(
                filters.map(|f| f.expr),
                partition_filters.map(|f| f.expr),
                columns.map(Arc::new),
                limit,
                None,
                aggregation.map(|f| f.expr),
            );
            Self(Arc::new(pushdowns))
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{:#?}", self.0))
        }

        #[getter]
        #[must_use]
        pub fn limit(&self) -> Option<usize> {
            self.0.limit
        }

        #[getter]
        #[must_use]
        pub fn filters(&self) -> Option<PyExpr> {
            self.0.filters.as_ref().map(|e| PyExpr { expr: e.clone() })
        }

        #[getter]
        #[must_use]
        pub fn partition_filters(&self) -> Option<PyExpr> {
            self.0
                .partition_filters
                .as_ref()
                .map(|e| PyExpr { expr: e.clone() })
        }

        #[getter]
        #[must_use]
        pub fn columns(&self) -> Option<Vec<String>> {
            self.0.columns.as_deref().cloned()
        }

        #[getter]
        #[must_use]
        pub fn aggregation(&self) -> Option<PyExpr> {
            self.0
                .aggregation
                .as_ref()
                .map(|e| PyExpr { expr: e.clone() })
        }

        pub fn filter_required_column_names(&self) -> Option<Vec<String>> {
            self.0
                .filters
                .as_ref()
                .map(daft_dsl::optimization::get_required_columns)
        }

        pub fn aggregation_required_column_names(&self) -> Option<Vec<String>> {
            self.0
                .aggregation
                .as_ref()
                .map(daft_dsl::optimization::get_required_columns)
        }

        pub fn aggregation_count_mode(&self) -> Option<CountMode> {
            self.0
                .aggregation
                .as_ref()
                .and_then(|expr| match expr.as_ref() {
                    Expr::Agg(AggExpr::Count(_, count_mode)) => Some(*count_mode),
                    _ => None,
                })
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<StorageConfig>()?;
    parent.add_class::<PyFileFormatConfig>()?;
    parent.add_class::<pylib::ScanOperatorHandle>()?;
    parent.add_class::<pylib::PyScanTask>()?;
    parent.add_class::<pylib_scan_info::PyPartitionField>()?;
    parent.add_class::<pylib_scan_info::PyPartitionTransform>()?;
    parent.add_class::<pylib_scan_info::PyPushdowns>()?;
    parent.add_class::<PyDataSource>()?;
    parent.add_class::<PyDataSourceTask>()?;

    Ok(())
}

pub fn register_testing_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(
        pylib::estimate_in_memory_size_bytes,
        parent
    )?)?;

    Ok(())
}
