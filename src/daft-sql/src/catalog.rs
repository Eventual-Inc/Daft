use std::{collections::HashMap, sync::Arc};

use daft_logical_plan::{LogicalPlan, LogicalPlanRef};

/// A simple map of table names to logical plans
#[derive(Debug, Clone)]
pub struct SQLCatalog {
    tables: HashMap<String, Arc<LogicalPlan>>,
}

impl SQLCatalog {
    /// Create an empty catalog
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a table with the catalog
    pub fn register_table(&mut self, name: &str, plan: LogicalPlanRef) {
        self.tables.insert(name.to_string(), plan);
    }

    /// Get a table from the catalog
    #[must_use]
    pub fn get_table(&self, name: &str) -> Option<LogicalPlanRef> {
        self.tables.get(name).cloned()
    }

    /// Copy from another catalog, using tables from other in case of conflict
    pub fn copy_from(&mut self, other: &Self) {
        for (name, plan) in &other.tables {
            self.tables.insert(name.clone(), plan.clone());
        }
    }
}

impl Default for SQLCatalog {
    fn default() -> Self {
        Self::new()
    }
}
