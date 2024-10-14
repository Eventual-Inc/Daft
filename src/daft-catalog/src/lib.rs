mod data_catalog;
mod data_catalog_table;
pub mod errors;

// Export public-facing traits
use std::{collections::HashMap, default, sync::Arc};

use daft_logical_plan::LogicalPlanBuilder;
pub use data_catalog::DataCatalog;
pub use data_catalog_table::DataCatalogTable;

#[cfg(feature = "python")]
pub mod python;

use errors::Error;

pub mod global_catalog {
    use std::sync::{Arc, Mutex};

    use lazy_static::lazy_static;

    use crate::{DaftMetaCatalog, DataCatalog};

    lazy_static! {
        pub(crate) static ref GLOBAL_DAFT_META_CATALOG: Mutex<DaftMetaCatalog> =
            Mutex::new(DaftMetaCatalog::new_from_env());
    }

    /// Register a DataCatalog with the global DaftMetaCatalog
    pub fn register_catalog(catalog: Arc<dyn DataCatalog>, name: Option<&str>) {
        GLOBAL_DAFT_META_CATALOG
            .lock()
            .unwrap()
            .register_catalog(catalog, name);
    }

    /// Unregisters a catalog with the global DaftMetaCatalog
    pub fn unregister_catalog(name: Option<&str>) -> bool {
        GLOBAL_DAFT_META_CATALOG
            .lock()
            .unwrap()
            .unregister_catalog(name)
    }
}

/// Name of the default catalog
static DEFAULT_CATALOG_NAME: &str = "default";

/// The [`DaftMetaCatalog`] is a catalog of [`DataCatalog`] implementations
///
/// Users of Daft can register various [`DataCatalog`] with Daft, enabling
/// discovery of tables across various [`DataCatalog`] implementations.
pub struct DaftMetaCatalog {
    /// Map of catalog names to the DataCatalog impls.
    ///
    /// NOTE: The default catalog is always named "default"
    data_catalogs: HashMap<String, Arc<dyn DataCatalog>>,

    /// LogicalPlans that were "named" and registered with Daft
    named_tables: HashMap<String, LogicalPlanBuilder>,
}

impl DaftMetaCatalog {
    /// Create a `DaftMetaCatalog` from the current environment
    pub fn new_from_env() -> Self {
        // TODO: Parse a YAML file to produce the catalog
        DaftMetaCatalog {
            data_catalogs: default::Default::default(),
            named_tables: default::Default::default(),
        }
    }

    /// Register a new [`DataCatalog`] with the `DaftMetaCatalog`.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The [`DataCatalog`] to register.
    pub fn register_catalog(&mut self, catalog: Arc<dyn DataCatalog>, name: Option<&str>) {
        let name = name.unwrap_or(DEFAULT_CATALOG_NAME);
        self.data_catalogs.insert(name.to_string(), catalog);
    }

    /// Unregister a [`DataCatalog`] from the `DaftMetaCatalog`.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the catalog to unregister. If None, the default catalog will be unregistered.
    ///
    /// # Returns
    ///
    /// Returns `true` if a catalog was successfully unregistered, `false` otherwise.
    pub fn unregister_catalog(&mut self, name: Option<&str>) -> bool {
        let name = name.unwrap_or(DEFAULT_CATALOG_NAME);
        self.data_catalogs.remove(name).is_some()
    }

    /// Registers a LogicalPlan with a name in the DaftMetaCatalog
    pub fn register_named_table(&mut self, name: &str, view: LogicalPlanBuilder) {
        self.named_tables.insert(name.to_string(), view);
    }

    /// Provides high-level functionality for reading a table of data against a [`DaftMetaCatalog`]
    ///
    /// Resolves the provided table_identifier against the catalog:
    ///
    /// 1. If there is an exact match for the provided `table_identifier` in the catalog's registered views, immediately return the exact match
    /// 2. If the [`DaftMetaCatalog`] has a default catalog, we will attempt to resolve the `table_identifier` against the default catalog
    /// 3. If the `table_identifier` is hierarchical (delimited by "."), use the first component as the Data Catalog name and resolve the rest of the components against
    ///     the selected Data Catalog
    pub fn read_table(
        &self,
        table_identifier: &str,
        catalog_name: Option<&str>,
    ) -> errors::Result<LogicalPlanBuilder> {
        // If the name is an exact match with a registered view, return it.
        if let Some(view) = self.named_tables.get(table_identifier) {
            return Ok(view.clone());
        }

        // Otherwise, check the catalogs for a match
        let catalog_name = catalog_name.unwrap_or(DEFAULT_CATALOG_NAME);
        if let Some(data_catalog) = self.data_catalogs.get(catalog_name) {
            if let Some(tbl) = data_catalog.get_table(table_identifier)? {
                tbl.as_ref().to_logical_plan_builder()
            } else {
                Err(Error::TableNotFound {
                    table_id: table_identifier.to_string(),
                })
            }
        } else {
            Err(Error::CatalogNotFound {
                name: catalog_name.to_string(),
            })
        }
    }
}
