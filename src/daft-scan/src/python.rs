use pyo3::prelude::*;

pub mod pylib {

    use daft_core::python::field::PyField;
    use daft_dsl::python::PyExpr;

    use daft_core::impl_bincode_py_state_serialization;
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;
    use pyo3::PyTypeInfo;

    use std::fmt::Display;

    use std::sync::Arc;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;
    use serde::{Deserialize, Serialize};

    use crate::anonymous::AnonymousScanOperator;
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
        pub fn from_python_abc(py_scan: PyObject) -> PyResult<Self> {
            let scan_op  =
                ScanOperatorRef(Arc::new(PythonScanOperatorBridge::from_python_abc(py_scan)?));
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
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pylib::ScanOperatorHandle>()?;
    parent.add_class::<pylib::PyScanTask>()?;
    parent.add_class::<pylib::PyPartitionField>()?;
    Ok(())
}
