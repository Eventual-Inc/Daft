mod error;

use daft_catalog::{identifier::Identifier, DaftCatalog};
use daft_logical_plan::LogicalPlanBuilder;

use crate::error::Result;

/// Temporary rename while refactoring.
type Metastore = DaftCatalog;

/// Session holds all state for query planning and execution (e.g. connection).
pub struct Session {
    /// Session identifier
    pub id: String,
    // TODO remove DaftCatalog after all APIs are migrated.
    pub metastore: DaftCatalog,
    // TODO execution context
    // TODO session options
}

impl Session {
    /// Creates a new empty session
    pub fn new(id: &str, metastore: Metastore) -> Self {
        Self {
            id: id.to_string(),
            metastore,
        }
    }

    /// Creates a table backed by the view
    /// TODO support names/namespaces and other table sources.
    pub fn create_table(&mut self, name: &str, view: impl Into<LogicalPlanBuilder>) -> Result<()> {
        self.metastore.register_table(name, view)
    }

    /// Gets a table by ident
    pub fn get_table(&self, ident: &Identifier) -> Result<LogicalPlanBuilder> {
        // TODO current read_table accepts an unparsed string
        let table_identifier = ident.to_string();
        // if ident.has_namespace() {
        //     unsupported!("qualified table names")
        // }
        self.metastore.read_table(&table_identifier)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self {
            id: "default".to_string(),
            metastore: Default::default(),
        }
    }
}
