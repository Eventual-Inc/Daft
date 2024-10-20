mod data_catalog;
mod data_catalog_table;
pub mod errors;

// Export public-facing traits
use std::{collections::HashMap, default, sync::Arc};

use daft_plan::LogicalPlanBuilder;
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
}

/// The [`DaftMetaCatalog`] is a catalog of [`DataCatalog`] implementations
///
/// Users of Daft can register various [`DataCatalog`] with Daft, enabling
/// discovery of tables across various [`DataCatalog`] implementations.
pub struct DaftMetaCatalog {
    default_data_catalog: Option<Arc<dyn DataCatalog>>,
    named_data_catalogs: HashMap<String, Arc<dyn DataCatalog>>,
    view_catalog: HashMap<String, LogicalPlanBuilder>,
}

impl DaftMetaCatalog {
    /// Create a `DaftMetaCatalog` from the current environment
    pub fn new_from_env() -> Self {
        // TODO: Parse a YAML file to produce the catalog
        DaftMetaCatalog {
            default_data_catalog: None,
            named_data_catalogs: default::Default::default(),
            view_catalog: default::Default::default(),
        }
    }

    /// Register a new [`DataCatalog`] with the `DaftMetaCatalog`.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The [`DataCatalog`] to register.
    /// * `name` - The name of the [`DataCatalog`], left as `None` if this is the default catalog
    pub fn register_catalog(&mut self, catalog: Arc<dyn DataCatalog>, name: Option<&str>) {
        match name {
            None => {
                self.default_data_catalog = Some(catalog);
            }
            Some(name) => {
                self.named_data_catalogs.insert(name.to_string(), catalog);
            }
        }
    }

    pub fn register_view(&mut self, name: &str, view: LogicalPlanBuilder) {
        self.view_catalog.insert(name.to_string(), view);
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
        if catalog_name.is_none() {
            if let Some(view) = self.view_catalog.get(table_identifier) {
                return Ok(view.clone());
            }
        }

        // Otherwise, look for
        let data_catalog = match catalog_name {
            None => self.default_data_catalog.as_ref(),
            Some(k) => self.named_data_catalogs.get(k),
        };

        if let Some(data_catalog) = data_catalog {
            let _table = data_catalog.get_table(table_identifier);
            todo!("Create a LogicalPlanBuilder from the DataCatalogTable");
        } else {
            Err(Error::CatalogNotFound {
                name: catalog_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "default".to_string()),
            })
        }
    }
}
