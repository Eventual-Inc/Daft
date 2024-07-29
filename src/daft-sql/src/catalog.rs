use std::{collections::HashMap, sync::Arc};

use daft_plan::{LogicalPlan, LogicalPlanRef};

/// A simple map of table names to logical plans
#[derive(Debug, Clone)]
pub struct SQLCatalog {
    tables: HashMap<String, Arc<LogicalPlan>>,
}

impl SQLCatalog {
    /// Create an empty catalog
    pub fn new() -> Self {
        SQLCatalog {
            tables: HashMap::new(),
        }
    }

    /// Register a table with the catalog
    pub fn register_table(&mut self, name: &str, plan: LogicalPlanRef) {
        self.tables.insert(name.to_string(), plan);
    }

    /// Get a table from the catalog
    pub fn get_table(&self, name: &str) -> Option<LogicalPlanRef> {
        self.tables.get(name).cloned()
    }
}

impl Default for SQLCatalog {
    fn default() -> Self {
        Self::new()
    }
}
