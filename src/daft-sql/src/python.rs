use daft_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use pyo3::prelude::*;

use crate::{catalog::SQLCatalog, planner::SQLPlanner};

#[pyfunction]
pub fn sql(sql: &str, catalog: PyCatalog) -> PyResult<PyLogicalPlanBuilder> {
    let planner = SQLPlanner::new(catalog.catalog);
    let plan = planner.plan_sql(sql)?;
    Ok(LogicalPlanBuilder::new(plan).into())
}

/// PyCatalog is the Python interface to the Catalog.
#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone)]
pub struct PyCatalog {
    catalog: SQLCatalog,
}

#[pymethods]
impl PyCatalog {
    /// Construct an empty PyCatalog.
    #[staticmethod]
    pub fn new() -> Self {
        PyCatalog {
            catalog: SQLCatalog::new(),
        }
    }

    /// Register a table with the catalog.
    pub fn register_table(&mut self, name: &str, dataframe: &mut PyLogicalPlanBuilder) {
        let plan = dataframe.builder.build();
        self.catalog.register_table(name, plan);
    }

    /// __str__ to print the catalog's tables
    fn __str__(&self) -> String {
        format!("{:?}", self.catalog)
    }
}

impl Default for PyCatalog {
    fn default() -> Self {
        Self::new()
    }
}
