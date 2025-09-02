use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

use crate::{
    functions::{FunctionEvaluator, FunctionExpr},
    ExprRef,
};

pub mod cache;
pub mod functions;
pub mod lance;
pub mod registry;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KVExpr {
    Lance(lance::LanceKVExpr),
}

impl KVExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            Self::Lance(expr) => expr,
        }
    }
}

/// KVConfig represents a unified configuration system for KV Store operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KVConfig {
    pub lance: Option<LanceConfig>,
}

impl KVConfig {
    pub fn new() -> Self {
        Self { lance: None }
    }

    pub fn with_lance(mut self, config: LanceConfig) -> Self {
        self.lance = Some(config);
        self
    }

    pub fn get_active_backend(&self) -> DaftResult<&str> {
        if self.lance.is_some() {
            Ok("lance")
        } else {
            Err(DaftError::ValueError(
                "No active KV backend configured".to_string(),
            ))
        }
    }
}

/// LanceConfig contains all parameters needed to create and access Lance datasets
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LanceConfig {
    pub uri: String,
    pub io_config: Option<IOConfig>,
    pub columns: Option<Vec<String>>,
    pub batch_size: usize,
    pub max_connections: usize,
}

impl LanceConfig {
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            io_config: None,
            columns: None,
            batch_size: 1000,
            max_connections: 32,
        }
    }

    pub fn with_io_config(mut self, io_config: IOConfig) -> Self {
        self.io_config = Some(io_config);
        self
    }

    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }
}

impl Default for KVConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionEvaluator for KVExpr {
    fn fn_name(&self) -> &'static str {
        self.get_evaluator().fn_name()
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        self.get_evaluator().to_field(inputs, schema, expr)
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        self.get_evaluator().evaluate(inputs, expr)
    }
}
