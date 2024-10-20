use daft_catalog::{errors::Result, DataCatalogTable};
use daft_plan::LogicalPlanBuilder;

/// Wrapper around PyIceberg, or None if PyIceberg is not installed
pub struct PyIcebergTable {} // TODO: Add a PyObject in here

/// Wrapper around PyIceberg, or None if PyIceberg is not installed
pub struct PyIcebergCatalog {} // TODO: Add a PyObject in here

impl DataCatalogTable for PyIcebergTable {
    fn to_logical_plan_builder(&self) -> Result<LogicalPlanBuilder> {
        todo!();
    }
}

impl PyIcebergCatalog {
    pub fn new_glue() -> Self {
        todo!("import the pyiceberg library and initialize a GlueCatalog");
    }

    pub fn load_table(&self) -> PyIcebergTable {
        todo!("Load a PyIcebergTable from the inner catalog object")
    }
}
