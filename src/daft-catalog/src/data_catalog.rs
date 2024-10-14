use crate::{data_catalog_table::DataCatalogTable, errors::Result};

/// DataCatalog is a catalog of data sources
///
/// It allows registering and retrieving data sources, as well as querying their schemas.
/// The catalog is used by the query planner to resolve table references in queries.
pub trait DataCatalog: Sync + Send {
    /// Lists the fully-qualified names of tables in the catalog with the specified prefix
    fn list_tables(&self, prefix: &str) -> Result<Vec<String>>;

    /// Retrieves a [`DataCatalogTable`] from this [`DataCatalog`] if it exists
    fn get_table(&self, name: &str) -> Result<Option<Box<dyn DataCatalogTable>>>;
}
