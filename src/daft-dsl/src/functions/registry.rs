use std::{collections::HashMap, sync::Arc};

use super::{ScalarFunction, ScalarUDF};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}

pub struct Registry {
    functions: HashMap<&'static str, Arc<dyn ScalarUDF>>,
}

impl Registry {
    fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }
    pub fn register(&mut self, function: Arc<dyn ScalarUDF>) {
        self.functions.insert(function.name(), function);
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn ScalarUDF>> {
        self.functions.get(name)
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.functions.keys().copied().collect()
    }
}