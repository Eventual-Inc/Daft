use pyo3::prelude::*;

pub mod pylib {
    use common_error::DaftResult;
    use daft_core::python::field::PyField;
    use daft_core::schema::SchemaRef;
    use daft_dsl::python::PyExpr;

    use daft_core::impl_bincode_py_state_serialization;
    use daft_stats::PartitionSpec;
    use daft_stats::TableMetadata;
    use daft_table::Table;
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;
    use pyo3::types::PyIterator;
    use pyo3::types::PyList;
    use pyo3::PyTypeInfo;

    use std::fmt::Display;

    use std::sync::Arc;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;
    use serde::{Deserialize, Serialize};

    use crate::anonymous::AnonymousScanOperator;
    use crate::DataFileSource;
    use crate::PartitionField;
    use crate::Pushdowns;
    use crate::ScanOperator;
    use crate::ScanOperatorRef;
    use crate::ScanTask;

    use crate::file_format::PyFileFormatConfig;
    use crate::glob::GlobScanOperator;
    use crate::storage_config::PyStorageConfig;

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
                Ok(ScanOperatorHandle {
                    scan_op: ScanOperatorRef(operator),
                })
            })
        }

        #[staticmethod]
        pub fn glob_scan(
            py: Python,
            glob_path: Vec<&str>,
            file_format_config: PyFileFormatConfig,
            storage_config: PyStorageConfig,
            schema: Option<PySchema>,
        ) -> PyResult<Self> {
            py.allow_threads(|| {
                let operator = Arc::new(GlobScanOperator::try_new(
                    glob_path.as_slice(),
                    file_format_config.into(),
                    storage_config.into(),
                    schema.map(|s| s.schema),
                )?);
                Ok(ScanOperatorHandle {
                    scan_op: ScanOperatorRef(operator),
                })
            })
        }

        #[staticmethod]
        pub fn from_python_scan_operator(py_scan: PyObject, py: Python) -> PyResult<Self> {
            let scan_op = ScanOperatorRef(Arc::new(PythonScanOperatorBridge::from_python_abc(
                py_scan, py,
            )?));
            Ok(ScanOperatorHandle { scan_op })
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
            Ok(Self {
                operator: abc,
                schema,
                partitioning_keys,
                can_absorb_filter,
                can_absorb_limit,
                can_absorb_select,
            })
        }
    }

    impl Display for PythonScanOperatorBridge {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "PythonScanOperator
operator:\n{:#?}
can_absorb_filter: {}
can_absorb_limit: {}
can_absorb_select: {}
schema:\n{}
partitioning_keys:\n",
                self.operator,
                self.can_absorb_filter,
                self.can_absorb_limit,
                self.can_absorb_select,
                self.schema
            )?;

            for p in self.partitioning_keys.iter() {
                writeln!(f, "{p}")?;
            }
            Ok(())
        }
    }

    impl ScanOperator for PythonScanOperatorBridge {
        fn partitioning_keys(&self) -> &[crate::PartitionField] {
            &self.partitioning_keys
        }
        fn schema(&self) -> daft_core::schema::SchemaRef {
            self.schema.clone()
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

        fn to_scan_tasks(
            &self,
            pushdowns: Pushdowns,
        ) -> common_error::DaftResult<
            Box<dyn Iterator<Item = common_error::DaftResult<crate::ScanTaskRef>>>,
        > {
            let scan_tasks = Python::with_gil(|py| {
                let pypd = PyPushdowns(pushdowns.into()).into_py(py);
                let pyiter =
                    self.operator
                        .call_method1(py, pyo3::intern!(py, "to_scan_tasks"), (pypd,))?;
                let pyiter = PyIterator::from_object(py, &pyiter)?;
                DaftResult::Ok(
                    pyiter
                        .map(|v| {
                            let pyscantask = v?.extract::<PyScanTask>()?.0;
                            DaftResult::Ok(pyscantask)
                        })
                        .collect::<Vec<_>>(),
                )
            })?;
            Ok(Box::new(scan_tasks.into_iter()))
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

        pub fn size_bytes(&self) -> PyResult<Option<i64>> {
            Ok(self.0.size_bytes().map(i64::try_from).transpose()?)
        }
    }

    #[pymethods]
    impl PyScanTask {
        #[staticmethod]
        pub fn catalog_scan_task(
            file: String,
            file_format: PyFileFormatConfig,
            schema: PySchema,
            num_rows: i64,
            storage_config: PyStorageConfig,
            size_bytes: Option<u64>,
            pushdowns: Option<PyPushdowns>,
        ) -> PyResult<Self> {
            // TODO(Sammy): This should parsed from the operator and passed in here
            let empty_pspec = PartitionSpec {
                keys: Table::empty(None)?,
            };
            let data_source = DataFileSource::CatalogDataFile {
                path: file,
                chunk_spec: None,
                size_bytes,
                metadata: TableMetadata {
                    length: num_rows as usize,
                },
                partition_spec: empty_pspec,
                statistics: None,
            };

            let scan_task = ScanTask::new(
                vec![data_source],
                file_format.into(),
                schema.schema,
                storage_config.into(),
                pushdowns.map(|p| p.0.as_ref().clone()).unwrap_or_default(),
            );
            Ok(PyScanTask(scan_task.into()))
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

    pub(crate) fn read_json_schema(
        py: Python,
        uri: &str,
        storage_config: PyStorageConfig,
    ) -> PyResult<PySchema> {
        py.import(pyo3::intern!(py, "daft.table.schema_inference"))?
            .getattr(pyo3::intern!(py, "from_json"))?
            .call1((uri, storage_config))?
            .getattr(pyo3::intern!(py, "_schema"))?
            .extract()
    }

    #[pyclass(module = "daft.daft", name = "PartitionField", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPartitionField(Arc<PartitionField>);

    #[pymethods]
    impl PyPartitionField {
        #[new]
        fn new(
            field: PyField,
            source_field: Option<PyField>,
            transform: Option<PyExpr>,
        ) -> PyResult<Self> {
            let p_field = PartitionField::new(
                field.field,
                source_field.map(|f| f.into()),
                transform.map(|e| e.expr),
            )?;
            Ok(PyPartitionField(Arc::new(p_field)))
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.0))
        }
    }

    #[pyclass(module = "daft.daft", name = "Pushdowns", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPushdowns(Arc<Pushdowns>);
    #[pymethods]
    impl PyPushdowns {
        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{:#?}", self.0))
        }
        #[getter]
        pub fn limit(&self) -> Option<usize> {
            self.0.limit
        }

        #[getter]
        pub fn filters(&self) -> Option<Vec<String>> {
            //TODO(Sammy): Figure out how to pass filters back to python
            None
        }

        #[getter]
        pub fn columns(&self) -> Option<Vec<String>> {
            self.0.columns.as_deref().cloned()
        }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pylib::ScanOperatorHandle>()?;
    parent.add_class::<pylib::PyScanTask>()?;
    parent.add_class::<pylib::PyPartitionField>()?;
    parent.add_class::<pylib::PyPushdowns>()?;
    Ok(())
}
