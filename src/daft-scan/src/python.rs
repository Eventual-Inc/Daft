use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use daft_core::impl_bincode_py_state_serialization;
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;
    use pyo3::PyTypeInfo;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;
    use serde::{Deserialize, Serialize};

    use crate::anonymous::AnonymousScanOperator;
    use crate::file_format::PyFileFormatConfig;
    use crate::glob::GlobScanOperator;
    use crate::storage_config::PyStorageConfig;
    use crate::{ScanOperatorRef, ScanTask};

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
                let runtime = daft_io::get_runtime(true)?;
                let _rt_guard = runtime.enter();
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
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pylib::ScanOperatorHandle>()?;
    parent.add_class::<pylib::PyScanTask>()?;
    Ok(())
}
