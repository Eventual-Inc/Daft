use daft_plan::{LogicalPlanBuilder, LogicalPlanRef};
use data_catalog::DataCatalog;

mod data_catalog;
mod errors;

#[cfg(feature = "python")]
pub mod python;

/// The [`DaftCatalog`] is a catalog of [`DataCatalog`] implementations
///
/// Users of Daft can register various [`DataCatalog`] with Daft, enabling
/// discovery of tables across various [`DataCatalog`] implementations.
pub struct DaftCatalog {}

impl DaftCatalog {
    /// Create a new, empty `DaftCatalog`.
    pub fn new_empty() -> Self {
        DaftCatalog {}
    }

    /// Create a DaftCatalog from a YAML file
    ///
    /// # Arguments
    ///
    /// * `yaml_file` - The path to the YAML file containing the catalog configuration.
    ///
    /// # Returns
    ///
    /// A new `DaftCatalog` instance configured according to the YAML file.
    pub fn from_yaml_file(_yaml_file: &str) -> Result<Self, common_error::DaftError> {
        todo!("Implement parsing a Daft catalog from YAML.")
    }

    /// Register a new [`DataCatalog`] with the `DaftCatalog`.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The [`DataCatalog`] to register.
    /// * `name` - The name of the [`DataCatalog`], left as `None` if this is the default catalog
    pub fn register_catalog(&mut self, _catalog: Box<dyn DataCatalog>, _name: Option<&str>) {
        todo!()
    }

    pub fn register_view(&mut self, _view: LogicalPlanRef) {
        todo!("Allow for registering views (expressed as LogicalPlans)");
    }
}

/// Provides high-level functionality for reading a table of data against a [`DaftCatalog`]
///
/// Resolves the provided table_identifier against the catalog:
///
/// 1. If there is an exact match for the provided `table_identifier` in the catalog's registered views, immediately return the exact match
/// 2. If the [`DaftCatalog`] has a default catalog, we will attempt to resolve the `table_identifier` against the default catalog
/// 3. If the `table_identifier` is hierarchical (delimited by "."), use the first component as the Data Catalog name and resolve the rest of the components against
///     the selected Data Catalog
pub fn read_table(
    _table_identifier: &str,
    _catalog: &DaftCatalog,
) -> errors::Result<LogicalPlanBuilder> {
    todo!("Implement read_table");
}
