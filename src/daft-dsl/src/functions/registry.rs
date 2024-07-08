use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use dashmap::DashMap;

use super::{hash::HashFunction, ScalarUDF};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}

pub struct Registry {
    functions: DashMap<&'static str, Arc<dyn ScalarUDF>>,
}

impl Registry {
    fn new() -> Self {
        let iter: Vec<(&'static str, Arc<dyn ScalarUDF>)> =
            vec![("hash", Arc::new(HashFunction {}))];

        Self {
            functions: iter.into_iter().collect(),
        }
    }
    pub fn register(&mut self, function: Arc<dyn ScalarUDF>) -> DaftResult<()> {
        if self.functions.contains_key(function.name()) {
            Err(DaftError::ValueError(format!(
                "function {} already exists",
                function.name()
            )))
        } else {
            self.functions.insert(function.name(), function);
            Ok(())
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ScalarUDF>> {
        self.functions.get(name).map(|f| f.value().clone())
    }

    pub fn names(&self) -> Vec<&'static str> {
        self.functions.iter().map(|pair| pair.name()).collect()
    }
}
