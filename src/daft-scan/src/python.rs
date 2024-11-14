use common_py_serde::{deserialize_py_object, serialize_py_object};
use pyo3::{prelude::*, types::PyTuple};
use serde::{Deserialize, Serialize};

use crate::storage_config::{NativeStorageConfig, PyStorageConfig, PythonStorageConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PyObjectSerializableWrapper(
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub PyObject,
);

/// Python arguments to a Python function that produces Tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonTablesFactoryArgs(Vec<PyObjectSerializableWrapper>);

impl PythonTablesFactoryArgs {
    pub fn new(args: Vec<PyObject>) -> Self {
        Self(args.into_iter().map(PyObjectSerializableWrapper).collect())
    }

    #[must_use]
    pub fn to_pytuple<'a>(&self, py: Python<'a>) -> Bound<'a, PyTuple> {
        pyo3::types::PyTuple::new_bound(py, self.0.iter().map(|x| x.0.bind(py)))
    }
}

impl PartialEq for PythonTablesFactoryArgs {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(s, o)| (s.0.as_ptr() as isize) == (o.0.as_ptr() as isize))
    }
}

pub mod pylib {
    use std::sync::Arc;

    use common_daft_config::{DaftExecutionConfig, PyDaftExecutionConfig};
    use common_error::DaftResult;
    use common_file_formats::{python::PyFileFormatConfig, FileFormatConfig};
    use common_py_serde::impl_bincode_py_state_serialization;
    use common_scan_info::{
        python::pylib::{PyPartitionField, PyPushdowns},
        PartitionField, Pushdowns, ScanOperator, ScanOperatorRef, ScanTaskLike, ScanTaskLikeRef,
    };
    use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
    use daft_schema::{python::schema::PySchema, schema::SchemaRef};
    use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
    use daft_table::{python::PyTable, Table};
    use pyo3::{
        prelude::*,
        pyclass,
        types::{PyIterator, PyList},
    };
    use serde::{Deserialize, Serialize};

    use super::PythonTablesFactoryArgs;
    use crate::{
        anonymous::AnonymousScanOperator,
        glob::GlobScanOperator,
        scan_task_iters::{merge_by_sizes, split_by_row_groups, BoxScanTaskIter},
        storage_config::{PyStorageConfig, PythonStorageConfig},
        DataSource, ScanTask,
    };
    #[pyclass(module = "daft.daft", frozen)]
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
            storage_config: PyStorageConfig,
        ) -> PyResult<Self> {
            py.allow_threads(|| {
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
        pub fn glob_scan(
            py: Python,
            glob_path: Vec<String>,
            file_format_config: PyFileFormatConfig,
            storage_config: PyStorageConfig,
            hive_partitioning: bool,
            infer_schema: bool,
            schema: Option<PySchema>,
            file_path_column: Option<String>,
        ) -> PyResult<Self> {
            py.allow_threads(|| {
                let operator = Arc::new(GlobScanOperator::try_new(
                    glob_path,
                    file_format_config.into(),
                    storage_config.into(),
                    infer_schema,
                    schema.map(|s| s.schema),
                    file_path_column,
                    hive_partitioning,
                )?);
                Ok(Self {
                    scan_op: ScanOperatorRef(operator),
                })
            })
        }

        #[staticmethod]
        pub fn from_python_scan_operator(py_scan: PyObject, py: Python) -> PyResult<Self> {
            let scan_op = ScanOperatorRef(Arc::new(PythonScanOperatorBridge::from_python_abc(
                py_scan, py,
            )?));
            Ok(Self { scan_op })
        }
    }
    #[pyclass(module = "daft.daft")]
    #[derive(Debug)]
    struct PythonScanOperatorBridge {
        operator: PyObject,
        schema: SchemaRef,
        partitioning_keys: Vec<PartitionField>,
        can_absorb_filter: bool,
        can_absorb_limit: bool,
        can_absorb_select: bool,
        display_name: String,
    }

    impl PythonScanOperatorBridge {
        fn _partitioning_keys(abc: &PyObject, py: Python) -> PyResult<Vec<PartitionField>> {
            let result = abc.call_method0(py, pyo3::intern!(py, "partitioning_keys"))?;
            let result = result.extract::<&PyList>(py)?;
            result
                .into_iter()
                .map(|p| Ok(p.extract::<PyPartitionField>()?.0.as_ref().clone()))
                .collect()
        }

        fn _schema(abc: &PyObject, py: Python) -> PyResult<SchemaRef> {
            let python_schema = abc.call_method0(py, pyo3::intern!(py, "schema"))?;
            let pyschema = python_schema
                .getattr(py, pyo3::intern!(py, "_schema"))?
                .extract::<PySchema>(py)?;
            Ok(pyschema.schema)
        }

        fn _can_absorb_filter(abc: &PyObject, py: Python) -> PyResult<bool> {
            abc.call_method0(py, pyo3::intern!(py, "can_absorb_filter"))?
                .extract::<bool>(py)
        }

        fn _can_absorb_limit(abc: &PyObject, py: Python) -> PyResult<bool> {
            abc.call_method0(py, pyo3::intern!(py, "can_absorb_limit"))?
                .extract::<bool>(py)
        }

        fn _can_absorb_select(abc: &PyObject, py: Python) -> PyResult<bool> {
            abc.call_method0(py, pyo3::intern!(py, "can_absorb_select"))?
                .extract::<bool>(py)
        }

        fn _display_name(abc: &PyObject, py: Python) -> PyResult<String> {
            abc.call_method0(py, pyo3::intern!(py, "display_name"))?
                .extract::<String>(py)
        }
    }

    #[pymethods]
    impl PythonScanOperatorBridge {
        #[staticmethod]
        pub fn from_python_abc(abc: PyObject, py: Python) -> PyResult<Self> {
            let partitioning_keys = Self::_partitioning_keys(&abc, py)?;
            let schema = Self::_schema(&abc, py)?;
            let can_absorb_filter = Self::_can_absorb_filter(&abc, py)?;
            let can_absorb_limit = Self::_can_absorb_limit(&abc, py)?;
            let can_absorb_select = Self::_can_absorb_select(&abc, py)?;
            let display_name = Self::_display_name(&abc, py)?;

            Ok(Self {
                operator: abc,
                schema,
                partitioning_keys,
                can_absorb_filter,
                can_absorb_limit,
                can_absorb_select,
                display_name,
            })
        }
    }

    impl ScanOperator for PythonScanOperatorBridge {
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

        fn multiline_display(&self) -> Vec<String> {
            let lines = vec![format!("PythonScanOperator: {}", self.display_name)];
            lines
        }

        fn to_scan_tasks(
            &self,
            pushdowns: Pushdowns,
            cfg: Option<&DaftExecutionConfig>,
        ) -> DaftResult<Vec<ScanTaskLikeRef>> {
            let scan_tasks = Python::with_gil(|py| {
                let pypd = PyPushdowns(pushdowns.clone().into()).into_py(py);
                let pyiter =
                    self.operator
                        .call_method1(py, pyo3::intern!(py, "to_scan_tasks"), (pypd,))?;
                let pyiter = PyIterator::from_bound_object(pyiter.bind(py))?;
                DaftResult::Ok(
                    pyiter
                        .map(|v| {
                            let pyscantask = v?.extract::<PyScanTask>()?.0;
                            DaftResult::Ok(pyscantask)
                        })
                        .collect::<Vec<_>>(),
                )
            })?;

            let mut scan_tasks: BoxScanTaskIter = Box::new(scan_tasks.into_iter());

            if let Some(cfg) = cfg {
                scan_tasks = split_by_row_groups(
                    scan_tasks,
                    cfg.parquet_split_row_groups_max_files,
                    cfg.scan_tasks_min_size_bytes,
                    cfg.scan_tasks_max_size_bytes,
                );

                scan_tasks = merge_by_sizes(scan_tasks, &pushdowns, cfg);
            }

            scan_tasks
                .map(|st| st.map(|task| task as Arc<dyn ScanTaskLike>))
                .collect()
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

    #[pyclass(module = "daft.daft", name = "ScanTask", frozen)]
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
        pub fn catalog_scan_task(
            file: String,
            file_format: PyFileFormatConfig,
            schema: PySchema,
            storage_config: PyStorageConfig,
            num_rows: Option<i64>,
            size_bytes: Option<u64>,
            iceberg_delete_files: Option<Vec<String>>,
            pushdowns: Option<PyPushdowns>,
            partition_values: Option<PyTable>,
            stats: Option<PyTable>,
        ) -> PyResult<Option<Self>> {
            if let Some(ref pvalues) = partition_values
                && let Some(Some(ref partition_filters)) =
                    pushdowns.as_ref().map(|p| &p.0.partition_filters)
            {
                let table = &pvalues.table;
                let eval_pred = table.eval_expression_list(&[partition_filters.clone()])?;
                assert_eq!(eval_pred.num_columns(), 1);
                let series = eval_pred.get_column_by_index(0)?;
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
                keys: partition_values.map_or_else(|| Table::empty(None).unwrap(), |p| p.table),
            };
            let statistics = stats
                .map(|s| TableStatistics::from_stats_table(&s.table))
                .transpose()?;

            let metadata = num_rows.map(|n| TableMetadata { length: n as usize });

            let data_source = DataSource::File {
                path: file,
                chunk_spec: None,
                size_bytes,
                iceberg_delete_files,
                metadata,
                partition_spec: Some(pspec),
                statistics,
                parquet_metadata: None,
            };

            let scan_task = ScanTask::new(
                vec![data_source],
                file_format.into(),
                schema.schema,
                storage_config.into(),
                pushdowns.map(|p| p.0.as_ref().clone()).unwrap_or_default(),
                None,
            );
            Ok(Some(Self(scan_task.into())))
        }

        #[allow(clippy::too_many_arguments)]
        #[staticmethod]
        pub fn sql_scan_task(
            url: String,
            file_format: PyFileFormatConfig,
            schema: PySchema,
            storage_config: PyStorageConfig,
            num_rows: Option<i64>,
            size_bytes: Option<u64>,
            pushdowns: Option<PyPushdowns>,
            stats: Option<PyTable>,
        ) -> PyResult<Self> {
            let statistics = stats
                .map(|s| TableStatistics::from_stats_table(&s.table))
                .transpose()?;
            let data_source = DataSource::Database {
                path: url,
                size_bytes,
                metadata: num_rows.map(|n| TableMetadata { length: n as usize }),
                statistics,
            };

            let scan_task = ScanTask::new(
                vec![data_source],
                file_format.into(),
                schema.schema,
                storage_config.into(),
                pushdowns.map(|p| p.0.as_ref().clone()).unwrap_or_default(),
                None,
            );
            Ok(Self(scan_task.into()))
        }

        #[allow(clippy::too_many_arguments)]
        #[staticmethod]
        pub fn python_factory_func_scan_task(
            py: Python,
            module: String,
            func_name: String,
            func_args: Vec<Bound<PyAny>>,
            schema: PySchema,
            num_rows: Option<i64>,
            size_bytes: Option<u64>,
            pushdowns: Option<PyPushdowns>,
            stats: Option<PyTable>,
        ) -> PyResult<Self> {
            let statistics = stats
                .map(|s| TableStatistics::from_stats_table(&s.table))
                .transpose()?;
            let data_source = DataSource::PythonFactoryFunction {
                module,
                func_name,
                func_args: PythonTablesFactoryArgs::new(
                    func_args.iter().map(|pyany| pyany.into_py(py)).collect(),
                ),
                size_bytes,
                metadata: num_rows.map(|num_rows| TableMetadata {
                    length: num_rows as usize,
                }),
                statistics,
                partition_spec: None,
            };

            let scan_task = ScanTask::new(
                vec![data_source],
                Arc::new(FileFormatConfig::PythonFunction),
                schema.schema,
                // HACK: StorageConfig isn't used when running the Python function but this is a non-optional arg for
                // ScanTask creation, so we just put in a placeholder here
                Arc::new(crate::storage_config::StorageConfig::Python(Arc::new(
                    PythonStorageConfig { io_config: None },
                ))),
                pushdowns.map(|p| p.0.as_ref().clone()).unwrap_or_default(),
                None,
            );
            Ok(Self(scan_task.into()))
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

    #[pyfunction]
    pub fn logical_plan_table_scan(
        scan_operator: ScanOperatorHandle,
    ) -> PyResult<PyLogicalPlanBuilder> {
        Ok(LogicalPlanBuilder::table_scan(scan_operator.into(), None)?.into())
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyStorageConfig>()?;
    parent.add_class::<NativeStorageConfig>()?;
    parent.add_class::<PythonStorageConfig>()?;

    parent.add_class::<pylib::ScanOperatorHandle>()?;
    parent.add_class::<pylib::PyScanTask>()?;
    parent.add_function(wrap_pyfunction_bound!(
        pylib::logical_plan_table_scan,
        parent
    )?)?;

    Ok(())
}
