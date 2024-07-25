use daft_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;

use crate::{analyzer, catalog::Catalog, parser::DaftParser};

#[pyfunction]
pub fn sql(sql: &str, catalog: PyCatalog) -> PyResult<PyLogicalPlanBuilder> {
    let ast = DaftParser::parse(sql).expect("parser error");
    let mut analyzer = analyzer::Analyzer::new(catalog.catalog);
    match analyzer.analyze(ast) {
        Ok(p) => Ok(p.into()),
        Err(e) => Err(e.into()),
    }
}

/// PyCatalog is the Python interface to the Catalog.
#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone)]
pub struct PyCatalog {
    catalog: Catalog,
}

#[pymethods]
impl PyCatalog {
    /// Construct an empty PyCatalog.
    #[staticmethod]
    pub fn new() -> Self {
        PyCatalog {
            catalog: Catalog::new(),
        }
    }

    /// Register a table with the catalog.
    pub fn register_table(&mut self, name: &str, dataframe: &mut PyLogicalPlanBuilder) {
        let plan = dataframe.builder.build();
        self.catalog.put_table(name, plan);
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
