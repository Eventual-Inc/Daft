use std::sync::Arc;

use daft_core::prelude::*;
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};
use indexmap::IndexMap;

use crate::error::{CatalogError, CatalogResult};

/// Table implementation reference.
pub type TableRef = Arc<dyn Table>;

/// Table sources for now are just references.
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum TableSource {
    /// Table source for CREATE TABLE t (<schema>)
    Schema(SchemaRef),
    /// Table source for CREATE TABLE t AS <view>
    View(LogicalPlanRef),
}

impl From<SchemaRef> for TableSource {
    fn from(schema: SchemaRef) -> Self {
        TableSource::Schema(schema)
    }
}

impl From<LogicalPlanRef> for TableSource {
    fn from(view: LogicalPlanRef) -> Self {
        TableSource::View(view)
    }
}

impl From<LogicalPlanBuilder> for TableSource {
    fn from(view: LogicalPlanBuilder) -> Self {
        TableSource::View(view.build())
    }
}

/// TODO consider moving out to daft-table, but this isn't necessary or helpful right now.
#[cfg(debug_assertions)]
pub trait Table: Sync + Send + std::fmt::Debug {
    /// Returns the table name.
    fn name(&self) -> String;

    /// Returns the table schema
    fn schema(&self) -> CatalogResult<SchemaRef>;

    /// Returns a logical plan for this table.
    fn to_logical_plan(&self) -> CatalogResult<LogicalPlanBuilder>;

    /// Append data to the table. Equivalent to `INSERT INTO` in SQL
    fn append(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, Literal>,
    ) -> CatalogResult<()>;
    /// Overwrite table with data. Equivalent to `INSERT OVERWRITE` in SQL
    fn overwrite(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, Literal>,
    ) -> CatalogResult<()>;

    /// Create/extract a Python object that subclasses the Table ABC
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}

#[cfg(not(debug_assertions))]
pub trait Table: Sync + Send {
    /// Returns the table name.
    fn name(&self) -> String;

    /// Returns the table schema
    fn schema(&self) -> CatalogResult<SchemaRef>;

    /// Returns a logical plan for this table.
    fn to_logical_plan(&self) -> CatalogResult<LogicalPlanBuilder>;

    /// Append data to the table. Equivalent to `INSERT INTO` in SQL
    fn append(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, Literal>,
    ) -> CatalogResult<()>;
    /// Overwrite table with data. Equivalent to `INSERT OVERWRITE` in SQL
    fn overwrite(
        &self,
        plan: LogicalPlanBuilder,
        options: IndexMap<String, Literal>,
    ) -> CatalogResult<()>;

    /// Create/extract a Python object that subclasses the Table ABC
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}

/// View is an immutable Table backed by a DataFrame.
#[derive(Clone)]
#[cfg_attr(feature = "python", pyo3::pyclass)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct View {
    name: String,
    plan: LogicalPlanBuilder,
}

impl View {
    pub fn new(name: impl Into<String>, plan: impl Into<LogicalPlanBuilder>) -> Self {
        Self {
            name: name.into(),
            plan: plan.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn plan(&self) -> LogicalPlanBuilder {
        self.plan.clone()
    }
}

impl Table for View {
    fn name(&self) -> String {
        self.name().to_string()
    }

    /// Returns a reference to the inner plan's schema
    fn schema(&self) -> CatalogResult<SchemaRef> {
        Ok(self.plan().schema())
    }

    /// Returns a reference to the inner plan
    fn to_logical_plan(&self) -> CatalogResult<LogicalPlanBuilder> {
        Ok(self.plan())
    }

    fn append(
        &self,
        _plan: LogicalPlanBuilder,
        _options: IndexMap<String, Literal>,
    ) -> CatalogResult<()> {
        Err(CatalogError::unsupported(
            "cannot modify the data in a view",
        ))
    }

    fn overwrite(
        &self,
        _plan: LogicalPlanBuilder,
        _options: IndexMap<String, Literal>,
    ) -> CatalogResult<()> {
        Err(CatalogError::unsupported(
            "cannot modify the data in a view",
        ))
    }

    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        use pyo3::{intern, types::PyAnyMethods};

        use crate::python::PyTable;

        let pytable = PyTable(Arc::new(self.clone()));

        Ok(py
            .import(intern!(py, "daft.catalog.__internal"))?
            .getattr("View")?
            .call1((pytable,))?
            .unbind())
    }
}
