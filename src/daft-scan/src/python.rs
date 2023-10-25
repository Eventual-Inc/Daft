use pyo3::prelude::*;

pub mod pylib {
    use pyo3::prelude::*;
    use std::str::FromStr;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;

    use crate::anonymous::AnonymousScanOperator;
    use crate::FileType;
    use crate::ScanOperatorRef;

    #[pyclass(module = "daft.daft", frozen)]
    pub(crate) struct ScanOperator {
        scan_op: ScanOperatorRef,
    }

    #[pymethods]
    impl ScanOperator {
        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.scan_op))
        }

        #[staticmethod]
        pub fn anonymous_scan(
            schema: PySchema,
            file_type: &str,
            files: Vec<String>,
        ) -> PyResult<Self> {
            let schema = schema.schema;
            let operator = Box::new(AnonymousScanOperator::new(
                schema,
                FileType::from_str(file_type)?,
                files,
            ));
            Ok(ScanOperator { scan_op: operator })
        }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pylib::ScanOperator>()?;
    Ok(())
}
