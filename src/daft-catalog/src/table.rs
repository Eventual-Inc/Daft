use std::sync::Arc;

use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder, LogicalPlanRef};

use crate::{bindings::Bindings, error::Result};

/// Table implementation reference.
pub type TableRef = Arc<dyn Table>;

/// TODO update this to accept both schemas and dataframes.
pub type TableSource = TableRef;


/// TableProvider is a collection of referenceable tables.
pub type TableProvider = Bindings<TableRef>;

/// TODO consider moving out to daft-table, but this isn't necessary or helpful right now.
pub trait Table: Sync + Send + std::fmt::Debug {
    // TODO get_name
    // TODO get_schema

    /// Returns a logical plan for this table.
    fn get_logical_plan(&self) -> Result<LogicalPlanBuilder>;

    /// Leverage dynamic dispatch to return the inner object for a PyTableImpl (generics?)
    #[cfg(feature = "python")]
    fn to_py(&self, _: pyo3::Python<'_>) -> pyo3::PyObject {
        panic!("missing to_py implementation, consider PyTable(self) as the blanket implementation")
    }
}

/// TODO update this once there's an actual table source
#[derive(Debug)]
pub struct TempTable(LogicalPlanRef);

impl Table for TempTable {
    fn get_logical_plan(&self) -> Result<LogicalPlanBuilder> {
        Ok(LogicalPlanBuilder::from(self.0.clone()))
    }
}

impl TempTable {
    pub fn source(plan: LogicalPlanRef) -> TableSource {
        Arc::new(TempTable(plan))
    }
}
