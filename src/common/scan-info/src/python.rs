use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use daft_dsl::python::PyExpr;
    use daft_schema::python::field::PyField;
    use pyo3::{prelude::*, pyclass};
    use serde::{Deserialize, Serialize};

    use crate::{PartitionField, PartitionTransform, Pushdowns};

    #[pyclass(module = "daft.daft", name = "PartitionField", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPartitionField(pub Arc<PartitionField>);

    #[pymethods]
    impl PyPartitionField {
        #[new]
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
    }

    #[pyclass(module = "daft.daft", name = "PartitionTransform", frozen)]
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

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.0))
        }
    }

    #[pyclass(module = "daft.daft", name = "Pushdowns", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPushdowns(pub Arc<Pushdowns>);
    #[pymethods]
    impl PyPushdowns {
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

        pub fn filter_required_column_names(&self) -> Option<Vec<String>> {
            self.0
                .filters
                .as_ref()
                .map(daft_dsl::optimization::get_required_columns)
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<pylib::PyPartitionField>()?;
    parent.add_class::<pylib::PyPartitionTransform>()?;
    parent.add_class::<pylib::PyPushdowns>()?;
    Ok(())
}
