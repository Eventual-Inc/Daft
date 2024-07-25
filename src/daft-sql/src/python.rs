use std::sync::Arc;

use daft_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;

use crate::{catalog::Catalog, parser::DaftParser};

#[pyfunction]
pub fn sql(sql: &str) -> PyResult<String> {
    let ast = DaftParser::parse(sql).expect("parser error");
    Ok(format!("{:?}", ast))
}

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone)]
pub struct PyCatalog {
    catalog: Arc<Catalog>,
}

#[pymethods]
impl PyCatalog {

    /// Construct an empty PySQLCatalog.
    #[staticmethod]
    pub fn new() -> Self {
        PyCatalog {
            catalog: Arc::new(Catalog::new()),
        }
    }

    /// Register a table with the catalog.
    pub fn register_table(&mut self, name: &str, df: &mut PyLogicalPlanBuilder) {
        todo!("register_table is not implemented");
        // let plan = df.builder.build();
        // self.catalog.put_table(name, plan);
    }
}
