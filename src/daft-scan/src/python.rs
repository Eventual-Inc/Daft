use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use pyo3::prelude::*;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;
    use serde::{Deserialize, Serialize};

    use crate::anonymous::AnonymousScanOperator;
    use crate::file_format::PyFileFormatConfig;
    use crate::storage_config::PyStorageConfig;
    use crate::{ScanOperatorRef, ScanTask, ScanTaskBatch};

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
            files: Vec<String>,
            schema: PySchema,
            file_format_config: PyFileFormatConfig,
            storage_config: PyStorageConfig,
        ) -> PyResult<Self> {
            let schema = schema.schema;
            let operator = Arc::new(AnonymousScanOperator::new(
                files,
                schema,
                file_format_config.into(),
                storage_config.into(),
            ));
            Ok(ScanOperatorHandle { scan_op: operator })
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
    pub struct PyScanTask(Arc<ScanTask>);

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

    #[pyclass(module = "daft.daft", name = "ScanTaskBatch", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyScanTaskBatch(Arc<ScanTaskBatch>);

    #[pymethods]
    impl PyScanTaskBatch {
        #[staticmethod]
        pub fn from_scan_tasks(scan_tasks: Vec<PyScanTask>) -> PyResult<Self> {
            let scan_tasks: Vec<ScanTask> = scan_tasks
                .into_iter()
                .map(|st| st.0.as_ref().clone())
                .collect();
            let scan_task_batch: ScanTaskBatch = scan_tasks.into();
            Ok(Self(Arc::new(scan_task_batch)))
        }

        pub fn num_rows(&self) -> PyResult<Option<i64>> {
            Ok(self.0.num_rows().map(i64::try_from).transpose()?)
        }

        pub fn size_bytes(&self) -> PyResult<Option<i64>> {
            Ok(self.0.size_bytes().map(i64::try_from).transpose()?)
        }
    }

    impl From<Arc<ScanTaskBatch>> for PyScanTaskBatch {
        fn from(value: Arc<ScanTaskBatch>) -> Self {
            Self(value)
        }
    }

    impl From<PyScanTaskBatch> for Arc<ScanTaskBatch> {
        fn from(value: PyScanTaskBatch) -> Self {
            value.0
        }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pylib::ScanOperatorHandle>()?;
    parent.add_class::<pylib::PyScanTask>()?;
    parent.add_class::<pylib::PyScanTaskBatch>()?;
    Ok(())
}
