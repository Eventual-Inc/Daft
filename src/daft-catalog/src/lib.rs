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

use errors::{Error, Result};

pub mod global_catalog {
    use std::sync::{Arc, RwLock};

    use lazy_static::lazy_static;

    use crate::{DaftCatalog, DataCatalog};

    lazy_static! {
        pub(crate) static ref GLOBAL_DAFT_META_CATALOG: RwLock<DaftCatalog> =
            RwLock::new(DaftCatalog::new_from_env());
    }

    /// Register a DataCatalog with the global DaftMetaCatalog
    pub fn register_catalog(catalog: Arc<dyn DataCatalog>, name: Option<&str>) {
        GLOBAL_DAFT_META_CATALOG
            .write()
            .unwrap()
            .register_catalog(catalog, name);
    }

    /// Unregisters a catalog with the global DaftMetaCatalog
    pub fn unregister_catalog(name: Option<&str>) -> bool {
        GLOBAL_DAFT_META_CATALOG
            .write()
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
#[derive(Debug, Clone, Default)]
pub struct DaftCatalog {
    /// Map of catalog names to the DataCatalog impls.
    ///
    /// NOTE: The default catalog is always named "default"
    data_catalogs: HashMap<String, Arc<dyn DataCatalog>>,

    /// LogicalPlans that were "named" and registered with Daft
    named_tables: HashMap<String, LogicalPlanBuilder>,
}

impl DaftCatalog {
    /// Create a `DaftMetaCatalog` from the current environment
    pub fn new_from_env() -> Self {
        // TODO: Parse a YAML file to produce the catalog
        DaftCatalog {
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
    pub fn register_table(
        &mut self,
        name: &str,
        view: impl Into<LogicalPlanBuilder>,
    ) -> Result<()> {
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(Error::InvalidTableName {
                name: name.to_string(),
            });
        }
        self.named_tables.insert(name.to_string(), view.into());
        Ok(())
    }

    /// Check if a named table is registered in the DaftCatalog
    pub fn contains_table(&self, name: &str) -> bool {
        self.named_tables.contains_key(name)
    }

    /// Provides high-level functionality for reading a table of data against a [`DaftMetaCatalog`]
    ///
    /// Resolves the provided table_identifier against the catalog:
    ///
    /// 1. If there is an exact match for the provided `table_identifier` in the catalog's registered named tables, immediately return the exact match
    /// 2. If the [`DaftMetaCatalog`] has a default catalog, we will attempt to resolve the `table_identifier` against the default catalog
    /// 3. If the `table_identifier` is hierarchical (delimited by "."), use the first component as the Data Catalog name and resolve the rest of the components against
    ///     the selected Data Catalog
    pub fn read_table(&self, table_identifier: &str) -> errors::Result<LogicalPlanBuilder> {
        // If the name is an exact match with a registered view, return it.
        if let Some(view) = self.named_tables.get(table_identifier) {
            return Ok(view.clone());
        }

        let mut searched_catalog_name = "default";
        let mut searched_table_name = table_identifier;

        // Check the default catalog for a match
        if let Some(default_data_catalog) = self.data_catalogs.get(DEFAULT_CATALOG_NAME) {
            if let Some(tbl) = default_data_catalog.get_table(table_identifier)? {
                return tbl.as_ref().to_logical_plan_builder();
            }
        }

        // Try to parse the catalog name from the provided table identifier by taking the first segment, split by '.'
        if let Some((catalog_name, table_name)) = table_identifier.split_once('.') {
            if let Some(data_catalog) = self.data_catalogs.get(catalog_name) {
                searched_catalog_name = catalog_name;
                searched_table_name = table_name;
                if let Some(tbl) = data_catalog.get_table(table_name)? {
                    return tbl.as_ref().to_logical_plan_builder();
                }
            }
        }

        // Return the error containing the last catalog/table pairing that we attempted to search on
        Err(Error::TableNotFound {
            catalog_name: searched_catalog_name.to_string(),
            table_id: searched_table_name.to_string(),
        })
    }
    /// Copy from another catalog, using tables from other in case of conflict
    pub fn copy_from(&mut self, other: &Self) {
        for (name, plan) in &other.named_tables {
            self.named_tables.insert(name.clone(), plan.clone());
        }
        for (name, catalog) in &other.data_catalogs {
            self.data_catalogs.insert(name.clone(), catalog.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::*;
    use daft_logical_plan::{
        ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanRef,
        SourceInfo,
    };

    use super::*;

    fn mock_plan() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("text", DataType::Utf8),
                Field::new("id", DataType::Int32),
            ])
            .unwrap(),
        );
        LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
                source_id: 0,
            })),
        ))
        .arced()
    }

    #[test]
    fn test_register_and_unregister_named_table() {
        let mut catalog = DaftCatalog::new_from_env();
        let plan = LogicalPlanBuilder::from(mock_plan());

        // Register a table
        assert!(catalog.register_table("test_table", plan.clone()).is_ok());

        // Try to register a table with invalid name
        assert!(catalog
            .register_table("invalid name", plan.clone())
            .is_err());
    }

    #[test]
    fn test_read_registered_table() {
        let mut catalog = DaftCatalog::new_from_env();
        let plan = LogicalPlanBuilder::from(mock_plan());

        catalog.register_table("test_table", plan).unwrap();

        assert!(catalog.read_table("test_table").is_ok());
        assert!(catalog.read_table("non_existent_table").is_err());
    }
}
