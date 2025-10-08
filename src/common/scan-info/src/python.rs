use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use daft_core::count_mode::CountMode;
    use daft_dsl::{AggExpr, Expr, python::PyExpr};
    use daft_schema::python::field::PyField;
    use pyo3::{exceptions::PyAttributeError, prelude::*, pyclass};
    use serde::{Deserialize, Serialize};

    use crate::{PartitionField, PartitionTransform, Pushdowns};

    #[pyclass(module = "daft.daft", name = "PyPartitionField", frozen)]
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

    #[pyclass(module = "daft.daft", name = "PyPartitionTransform", frozen)]
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

    #[pyclass(module = "daft.daft", name = "PyPushdowns", frozen)]
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
            match self.0.aggregation.as_ref() {
                Some(expr) => match expr.as_ref() {
                    Expr::Agg(AggExpr::Count(_, count_mode)) => Some(*count_mode),
                    _ => None,
                },
                None => None,
            }
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<pylib::PyPartitionField>()?;
    parent.add_class::<pylib::PyPartitionTransform>()?;
    parent.add_class::<pylib::PyPushdowns>()?;
    Ok(())
}
