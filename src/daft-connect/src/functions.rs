use std::{collections::HashMap, sync::Arc};

use once_cell::sync::Lazy;
use spark_connect::Expression;

use crate::{error::ConnectResult, spark_analyzer::SparkAnalyzer};
mod core;

pub(crate) static CONNECT_FUNCTIONS: Lazy<SparkFunctions> = Lazy::new(|| {
    let mut functions = SparkFunctions::new();
    functions.register::<core::CoreFunctions>();
    functions
});

pub trait SparkFunction: Send + Sync {
    fn to_expr(
        &self,
        args: &[Expression],
        analyzer: &SparkAnalyzer,
    ) -> ConnectResult<daft_dsl::ExprRef>;
}

pub struct SparkFunctions {
    pub(crate) map: HashMap<String, Arc<dyn SparkFunction>>,
}

impl SparkFunctions {
    /// Create a new [SparkFunction] instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Register the module to the [SparkFunctions] instance.
    pub fn register<M: FunctionModule>(&mut self) {
        M::register(self);
    }
    /// Add a [FunctionExpr] to the [SparkFunction] instance.
    pub fn add_fn<F: SparkFunction + 'static>(&mut self, name: &str, func: F) {
        self.map.insert(name.to_string(), Arc::new(func));
    }

    /// Get a function by name from the [SparkFunctions] instance.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SparkFunction>> {
        self.map.get(name)
    }
}

pub trait FunctionModule {
    /// Register this module to the given [SparkFunctions] table.
    fn register(_parent: &mut SparkFunctions);
}
