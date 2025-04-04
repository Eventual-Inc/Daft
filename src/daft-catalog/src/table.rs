use std::sync::Arc;

use daft_core::prelude::SchemaRef;
use daft_logical_plan::{LogicalPlanBuilder, LogicalPlanRef};

use crate::error::Result;

/// Table implementation reference.
pub type TableRef = Arc<dyn Table>;

/// Table sources for now are just references.
#[derive(Debug, Clone)]
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
pub trait Table: Sync + Send + std::fmt::Debug {
    /// Returns the table schema
    fn get_schema(&self) -> SchemaRef;

    /// Returns a logical plan for this table.
    fn get_logical_plan(&self) -> Result<LogicalPlanRef>;

    /// Leverage dynamic dispatch to return the inner object for a PyTableImpl (generics?)
    #[cfg(feature = "python")]
    fn to_py(&self, _: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::PyObject> {
        panic!("missing to_py implementation, consider PyTable(self) as the blanket implementation")
    }
}

/// View is an immutable Table backed by a DataFrame.
#[derive(Debug, Clone)]
pub struct View(LogicalPlanRef);

impl From<LogicalPlanRef> for View {
    fn from(plan: LogicalPlanRef) -> Self {
        Self(plan)
    }
}

impl From<LogicalPlanBuilder> for View {
    fn from(value: LogicalPlanBuilder) -> Self {
        Self(value.plan)
    }
}

impl View {
    pub fn arced(self) -> Arc<View> {
        Arc::new(self)
    }
}

impl Table for View {
    /// Returns a reference to the inner plan's schema
    fn get_schema(&self) -> SchemaRef {
        self.0.schema().clone()
    }

    /// Returns a reference to the inner plan
    fn get_logical_plan(&self) -> Result<LogicalPlanRef> {
        Ok(self.0.clone())
    }

    /// This is a little ugly .. it creates a PyObject which implements the daft.catalog.Table ABC
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::PyObject> {
        use pyo3::{types::PyAnyMethods, IntoPyObject};

        use crate::python::PyTable;
        PyTable::new(self.clone().arced())
            .into_pyobject(py)?
            .extract()
    }
}
