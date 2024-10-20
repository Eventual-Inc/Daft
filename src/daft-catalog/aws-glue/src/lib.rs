#[cfg(feature = "python")]
pub mod python;

struct AWSGlueCatalog {}

impl daft_catalog::DataCatalog for AWSGlueCatalog {
    fn list_tables(&self, _prefix: &str) -> Vec<String> {
        todo!();
    }

    fn get_table(&self, _name: &str) -> Option<Box<dyn daft_catalog::DataCatalogTable>> {
        // Make a request to AWS Glue to find the table
        // If the table metadata indicates that this is an iceberg table, then delegate to PyIceberg to read
        // NOTE: we have to throw an import error if the pyiceberg_catalog isn't instantiated, indicating that PyIceberg
        // is not installed.
        todo!("Detect from the table metadata that this is an iceberg table, then delegate to pyiceberg");
    }
}

impl AWSGlueCatalog {
    pub fn new() -> Self {
        // Naively instantiate this, which under the hood may naively be a no-op if PyIceberg isn't installed
        // let pyiceberg_catalog = PyIcebergCatalog::new_glue(); // TODO

        AWSGlueCatalog {}
    }
}
