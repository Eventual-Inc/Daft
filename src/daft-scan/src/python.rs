use pyo3::prelude::*;

pub mod pylib {

    use daft_dsl::python::PyExpr;

    use pyo3::prelude::*;

    use std::fmt::Display;

    use std::sync::Arc;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;
    use serde::{Deserialize, Serialize};

    use crate::anonymous::AnonymousScanOperator;
    use crate::Pushdowns;
    use crate::ScanOperator;
    use crate::ScanOperatorRef;
    use crate::ScanTask;
    use crate::ScanTaskBatch;

    use crate::file_format::PyFileFormatConfig;
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

        #[staticmethod]
        pub fn from_python_abc(py_scan: PyObject) -> PyResult<Self> {
            let scan_op: ScanOperatorRef =
                Arc::new(PythonScanOperatorBridge::from_python_abc(py_scan)?);
            Ok(ScanOperatorHandle { scan_op })
        }
    }
    #[pyclass(module = "daft.daft")]
    #[derive(Debug)]
    struct PythonScanOperatorBridge {
        operator: PyObject,
    }
    #[pymethods]
    impl PythonScanOperatorBridge {
        #[staticmethod]
        pub fn from_python_abc(abc: PyObject) -> PyResult<Self> {
            Ok(Self { operator: abc })
        }

        pub fn _filter(&self, py: Python, predicate: PyExpr) -> PyResult<(bool, Self)> {
            let _from_pyexpr = py
                .import(pyo3::intern!(py, "daft.expressions"))?
                .getattr(pyo3::intern!(py, "Expression"))?
                .getattr(pyo3::intern!(py, "_from_pyexpr"))?;
            let expr = _from_pyexpr.call1((predicate,))?;
            let result = self.operator.call_method(py, "filter", (expr,), None)?;
            let (absorb, new_op) = result.extract::<(bool, PyObject)>(py)?;
            Ok((absorb, Self { operator: new_op }))
        }
    }

    impl Display for PythonScanOperatorBridge {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:#?}", self)
        }
    }

    impl ScanOperator for PythonScanOperatorBridge {
        // fn filter(
        //     self: Box<Self>,
        //     predicate: &daft_dsl::Expr,
        // ) -> common_error::DaftResult<(bool, ScanOperatorRef)> {
        //     Python::with_gil(|py| {
        //         let (can, new_op) = self._filter(
        //             py,
        //             PyExpr {
        //                 expr: predicate.clone(),
        //             },
        //         )?;
        //         Ok((can, Box::new(new_op) as ScanOperatorRef))
        //     })
        // }
        // fn limit(self: Box<Self>, num: usize) -> common_error::DaftResult<ScanOperatorRef> {
        //     todo!()
        // }
        // fn num_partitions(&self) -> common_error::DaftResult<usize> {
        //     todo!()
        // }
        fn partitioning_keys(&self) -> &[crate::PartitionField] {
            todo!()
        }
        fn schema(&self) -> daft_core::schema::SchemaRef {
            todo!()
        }
        fn can_absorb_filter(&self) -> bool {
            todo!()
        }
        fn can_absorb_limit(&self) -> bool {
            todo!()
        }
        fn can_absorb_select(&self) -> bool {
            todo!()
        }

        // fn select(self: Box<Self>, columns: &[&str]) -> common_error::DaftResult<ScanOperatorRef> {
        //     todo!()
        // }
        fn to_scan_tasks(
            &self,
            _pushdowns: Pushdowns,
        ) -> common_error::DaftResult<
            Box<dyn Iterator<Item = common_error::DaftResult<crate::ScanTask>>>,
        > {
            todo!()
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
