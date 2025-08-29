use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_io::IOConfig;

use crate::functions::{
    scalar::{ScalarFunction, ScalarUDF},
    FunctionArgs,
};

use super::{lance::LanceKVExpr, KVConfig, LanceConfig};

/// Lance KV Get function
#[derive(Debug, Clone)]
pub struct LanceKVGet;

impl ScalarUDF for LanceKVGet {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "lance_kv_get"
    }

    fn to_field(&self, inputs: &[Field], _args: &FunctionArgs) -> DaftResult<Field> {
        match inputs {
            [input] => Ok(Field::new(input.name.clone(), DataType::Binary)),
            _ => Err(DaftError::ValueError(
                "lance_kv_get expects exactly one input (row_id column)".to_string(),
            )),
        }
    }

    fn evaluate(&self, inputs: &[Series], args: &FunctionArgs) -> DaftResult<Series> {
        match inputs {
            [row_ids] => {
                // Extract arguments
                let uri = args.get_required("uri")?;
                let columns = args.try_get_optional("columns")?;
                let on_error = args.get_optional("on_error").unwrap_or("raise".to_string());
                let io_config = args.try_get_optional::<IOConfig>("io_config")?;

                // Create Lance KV expression
                let lance_expr = LanceKVExpr::get(
                    uri,
                    columns,
                    on_error,
                    io_config,
                );

                // Evaluate the expression
                lance_expr.evaluate(&[row_ids.clone()], &crate::functions::FunctionExpr::KV(
                    super::KVExpr::Lance(lance_expr)
                ))
            }
            _ => Err(DaftError::ValueError(
                "lance_kv_get expects exactly one input (row_id column)".to_string(),
            )),
        }
    }
}

/// Lance KV Take function
#[derive(Debug, Clone)]
pub struct LanceKVTake;

impl ScalarUDF for LanceKVTake {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "lance_kv_take"
    }

    fn to_field(&self, inputs: &[Field], _args: &FunctionArgs) -> DaftResult<Field> {
        match inputs {
            [input] => Ok(Field::new(input.name.clone(), DataType::Binary)),
            _ => Err(DaftError::ValueError(
                "lance_kv_take expects exactly one input (row_id column)".to_string(),
            )),
        }
    }

    fn evaluate(&self, inputs: &[Series], args: &FunctionArgs) -> DaftResult<Series> {
        match inputs {
            [row_ids] => {
                // Extract arguments
                let uri = args.get_required("uri")?;
                let columns = args.try_get_optional("columns")?;
                let batch_size = args.get_optional("batch_size").unwrap_or(1000);
                let on_error = args.get_optional("on_error").unwrap_or("raise".to_string());
                let io_config = args.try_get_optional::<IOConfig>("io_config")?;

                // Create Lance KV expression
                let lance_expr = LanceKVExpr::take(
                    uri,
                    columns,
                    batch_size,
                    on_error,
                    io_config,
                );

                // Evaluate the expression
                lance_expr.evaluate(&[row_ids.clone()], &crate::functions::FunctionExpr::KV(
                    super::KVExpr::Lance(lance_expr)
                ))
            }
            _ => Err(DaftError::ValueError(
                "lance_kv_take expects exactly one input (row_id column)".to_string(),
            )),
        }
    }
}

/// Lance KV Exists function
#[derive(Debug, Clone)]
pub struct LanceKVExists;

impl ScalarUDF for LanceKVExists {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "lance_kv_exists"
    }

    fn to_field(&self, inputs: &[Field], _args: &FunctionArgs) -> DaftResult<Field> {
        match inputs {
            [input] => Ok(Field::new(input.name.clone(), DataType::Boolean)),
            _ => Err(DaftError::ValueError(
                "lance_kv_exists expects exactly one input (row_id column)".to_string(),
            )),
        }
    }

    fn evaluate(&self, inputs: &[Series], args: &FunctionArgs) -> DaftResult<Series> {
        match inputs {
            [row_ids] => {
                // Extract arguments
                let uri = args.get_required("uri")?;
                let on_error = args.get_optional("on_error").unwrap_or("raise".to_string());
                let io_config = args.try_get_optional::<IOConfig>("io_config")?;

                // Create Lance KV expression
                let lance_expr = LanceKVExpr::exists(uri, on_error, io_config);

                // Evaluate the expression
                lance_expr.evaluate(&[row_ids.clone()], &crate::functions::FunctionExpr::KV(
                    super::KVExpr::Lance(lance_expr)
                ))
            }
            _ => Err(DaftError::ValueError(
                "lance_kv_exists expects exactly one input (row_id column)".to_string(),
            )),
        }
    }
}

/// KV Config creation function
#[derive(Debug, Clone)]
pub struct CreateKVConfig;

impl ScalarUDF for CreateKVConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "create_kv_config"
    }

    fn to_field(&self, _inputs: &[Field], _args: &FunctionArgs) -> DaftResult<Field> {
        // Return a struct type representing KVConfig
        Ok(Field::new("kv_config", DataType::Struct(vec![
            Field::new("backend", DataType::Utf8),
            Field::new("config", DataType::Binary),
        ])))
    }

    fn evaluate(&self, _inputs: &[Series], args: &FunctionArgs) -> DaftResult<Series> {
        // Extract configuration parameters
        let backend = args.get_optional("backend").unwrap_or("lance".to_string());
        
        match backend.as_str() {
            "lance" => {
                let uri = args.get_required("uri")?;
                let io_config = args.try_get_optional::<IOConfig>("io_config")?;
                let columns = args.try_get_optional::<Vec<String>>("columns")?;
                let batch_size = args.get_optional("batch_size").unwrap_or(1000);
                let max_connections = args.get_optional("max_connections").unwrap_or(32);

                // Create Lance config
                let mut lance_config = LanceConfig::new(uri)
                    .with_batch_size(batch_size)
                    .with_max_connections(max_connections);

                if let Some(io_config) = io_config {
                    lance_config = lance_config.with_io_config(io_config);
                }

                if let Some(columns) = columns {
                    lance_config = lance_config.with_columns(columns);
                }

                // Create KV config
                let kv_config = KVConfig::new().with_lance(lance_config);

                // Serialize config to binary
                let serialized = serde_json::to_vec(&kv_config)
                    .map_err(|e| DaftError::ValueError(format!("Failed to serialize KVConfig: {}", e)))?;

                // Create struct series
                let backend_series = Series::try_from(("backend", vec!["lance"]))?;
                let config_series = Series::try_from(("config", vec![Some(serialized)]))?;

                Ok(Series::try_from_field_and_series(
                    Field::new("kv_config", DataType::Struct(vec![
                        Field::new("backend", DataType::Utf8),
                        Field::new("config", DataType::Binary),
                    ])),
                    vec![backend_series, config_series],
                )?)
            }
            _ => Err(DaftError::ValueError(
                format!("Unsupported KV backend: {}", backend)
            )),
        }
    }
}