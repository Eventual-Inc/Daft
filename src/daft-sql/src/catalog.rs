use std::{collections::HashMap, sync::Arc};

use daft_plan::LogicalPlan;

/// A simple map of table names to logical plans
#[derive(Debug, Clone)]
pub struct Catalog {
    tables: HashMap<String, Arc<LogicalPlan>>,
}

impl Catalog {
    /// Create a new catalog
    pub fn new() -> Self {
        Catalog { tables: HashMap::new() }
    }

    /// Associate a table name (string) with its logical plan.
    pub fn put_table(&mut self, name: &str, plan: Arc<LogicalPlan>) {
        self.tables.insert(name.to_string(), plan);
    }

    // Return a logical plan associated with a table name.
    // pub fn get_table(&self, name: &str) -> Option<Arc<LogicalPlan>> {
    //     self.tables.get(name)
    // }
}
