use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use super::{KVConfig, LanceConfig, lance::LanceKVExpr};
use crate::{
    ExprRef,
    functions::{
        function_args::{FunctionArgs, FunctionArgs as FunctionArgsDerive},
        scalar::ScalarUDF,
    },
};

/// Helper function to deserialize KVConfig from Series
fn deserialize_kv_config_from_series(kv_config_series: &Series) -> DaftResult<KVConfig> {
    // Try to extract the KVConfig from the series
    // The series should contain serialized KVConfig data
    if kv_config_series.len() != 1 {
        let err_msg = format!(
            "KVConfig series must contain exactly one element, got {}",
            kv_config_series.len()
        );
        return Err(DaftError::ValueError(err_msg));
    }

    let binary_array = kv_config_series.binary()?;

    let binary_data = binary_array
        .get(0)
        .ok_or_else(|| DaftError::ValueError("KVConfig series contains no data".to_string()))?;

    // Deserialize the JSON data
    let kv_config: KVConfig = serde_json::from_slice(binary_data)
        .map_err(|e| DaftError::ValueError(format!("Failed to deserialize KVConfig: {}", e)))?;
    Ok(kv_config)
}

/// Helper function to extract parameters from KVConfig
fn extract_lance_params_from_config(
    kv_config: &KVConfig,
) -> DaftResult<(String, Option<Vec<String>>, usize, String)> {
    let lance_config = kv_config.lance.as_ref().ok_or_else(|| {
        DaftError::ValueError("No Lance configuration found in KVConfig".to_string())
    })?;

    let uri = lance_config.uri.clone();
    let columns = lance_config.columns.clone();
    let batch_size = lance_config.batch_size;
    let on_error = "raise".to_string(); // Default error handling

    Ok((uri, columns, batch_size, on_error))
}

/// Arguments for KV Get with Config function
#[derive(FunctionArgsDerive)]
struct KVGetWithConfigArgs<T> {
    row_ids: T,

    #[arg(optional)]
    kv_config: Option<T>,
}

/// KV Get with Config function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVGetWithConfig;

#[typetag::serde]
impl ScalarUDF for KVGetWithConfig {
    fn name(&self) -> &'static str {
        "kv_get_with_config"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        // Try multiple debug approaches
        std::fs::write(
            "/tmp/rust_debug.log",
            "DEBUG: KVGetWithConfig::call invoked\n",
        )
        .ok();
        eprintln!("DEBUG: KVGetWithConfig::call invoked");

        // Also try writing to a different location
        std::fs::write(
            "/workspace/iris_59ecff5f-dd8f-4eb8-a92d-dca852586066/rust_debug.log",
            "DEBUG: KVGetWithConfig::call invoked\n",
        )
        .ok();

        let KVGetWithConfigArgs {
            row_ids: _row_ids,
            kv_config,
        } = args.try_into()?;

        std::fs::write(
            "/tmp/rust_debug.log",
            "DEBUG: Arguments parsed successfully\n",
        )
        .ok();
        let debug_msg = format!("DEBUG: kv_config is_some: {}\n", kv_config.is_some());
        std::fs::write("/tmp/rust_debug.log", debug_msg).ok();

        // Extract parameters from KVConfig if provided
        let (uri_str, columns_list, _batch_size, on_error_str) =
            if let Some(kv_config_series) = kv_config {
                std::fs::write(
                    "/tmp/rust_debug.log",
                    "DEBUG: KVConfig provided, attempting deserialization\n",
                )
                .ok();
                // Force error propagation instead of silent fallback
                let config = deserialize_kv_config_from_series(&kv_config_series).map_err(|e| {
                    let debug_msg =
                        format!("DEBUG: KVConfig deserialization failed with error: {}\n", e);
                    std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
                    DaftError::ValueError(format!("KVConfig deserialization failed: {}", e))
                })?;
                std::fs::write(
                    "/tmp/rust_debug.log",
                    "DEBUG: KVConfig deserialized successfully, extracting Lance params\n",
                )
                .ok();
                let result = extract_lance_params_from_config(&config).map_err(|e| {
                    let debug_msg =
                        format!("DEBUG: Lance params extraction failed with error: {}\n", e);
                    std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
                    DaftError::ValueError(format!("Lance params extraction failed: {}", e))
                })?;
                let debug_msg = format!(
                    "DEBUG: Lance params extracted: uri={}, columns={:?}\n",
                    result.0, result.1
                );
                std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
                result
            } else {
                std::fs::write(
                    "/tmp/rust_debug.log",
                    "DEBUG: No KVConfig provided, using fallback values\n",
                )
                .ok();
                // Fallback to default values if no config provided
                ("dummy_uri".to_string(), None, 1000, "raise".to_string())
            };

        let debug_msg = format!("DEBUG: Final URI to be used: {}\n", uri_str);
        std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
        let io_config_opt = None; // TODO: Extract from KVConfig when IOConfig is supported

        // Create Lance KV expression with extracted parameters
        let lance_expr = LanceKVExpr::get(uri_str, columns_list, on_error_str, io_config_opt);

        // Directly call the Lance implementation
        match &lance_expr {
            LanceKVExpr::Get {
                uri,
                columns: _columns,
                on_error: _on_error,
                io_config: _io_config,
            } => {
                let debug_msg =
                    format!("DEBUG: About to call lance_kv_get_impl with URI: {}\n", uri);
                std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
                // TODO: Implement Lance KV get operation
                // This is a framework placeholder for the actual implementation
                Ok(Series::empty("result", &DataType::Binary))
            }
            _ => Err(DaftError::ValueError(
                "Invalid Lance KV expression type for get operation".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let KVGetWithConfigArgs {
            row_ids: _row_ids,
            kv_config: _,
        } = args.try_into()?;

        let input_field = _row_ids.to_field(schema)?;
        Ok(Field::new(input_field.name, DataType::Binary))
    }
}

/// Arguments for KV Batch Get with Config function
#[derive(FunctionArgsDerive)]
struct KVBatchGetWithConfigArgs<T> {
    row_ids: T,

    #[arg(optional)]
    kv_config: Option<T>,

    #[arg(optional)]
    batch_size: Option<T>,
}

/// KV Batch Get with Config function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVBatchGetWithConfig;

#[typetag::serde]
impl ScalarUDF for KVBatchGetWithConfig {
    fn name(&self) -> &'static str {
        "kv_batch_get_with_config"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let KVBatchGetWithConfigArgs {
            row_ids: _row_ids,
            kv_config,
            batch_size,
        } = args.try_into()?;

        // Extract batch size from parameter or use default from config
        let batch_size_val = if let Some(ref batch_size_series) = batch_size {
            if batch_size_series.len() == 1 {
                // Handle both Int32 and Int64 types for batch_size
                let batch_size_value = match batch_size_series.data_type() {
                    DataType::Int32 => {
                        let batch_size_array = batch_size_series.i32()?;
                        batch_size_array.get(0).unwrap_or(1000) as i64
                    }
                    DataType::Int64 => {
                        let batch_size_array = batch_size_series.i64()?;
                        batch_size_array.get(0).unwrap_or(1000)
                    }
                    _ => 1000i64,
                };
                batch_size_value as usize
            } else {
                1000
            }
        } else {
            1000
        };

        // Extract parameters from KVConfig if provided
        let (uri_str, columns_list, config_batch_size, on_error_str) =
            if let Some(kv_config_series) = kv_config {
                let config = deserialize_kv_config_from_series(&kv_config_series)?;
                extract_lance_params_from_config(&config)?
            } else {
                // Fallback to default values if no config provided
                ("dummy_uri".to_string(), None, 1000, "raise".to_string())
            };

        // Use explicit batch_size parameter if provided, otherwise use config value
        let final_batch_size = if batch_size.is_some() {
            batch_size_val
        } else {
            config_batch_size
        };

        let io_config_opt = None; // TODO: Extract from KVConfig when IOConfig is supported

        // Create Lance KV expression with extracted parameters
        let lance_expr = LanceKVExpr::batch_get(
            uri_str,
            columns_list,
            final_batch_size,
            on_error_str,
            io_config_opt,
        );

        // Directly call the Lance implementation
        match &lance_expr {
            LanceKVExpr::BatchGet {
                uri: _uri,
                columns: _columns,
                batch_size: _batch_size,
                on_error: _on_error,
                io_config: _io_config,
            } => {
                // TODO: Implement Lance KV batch_get operation
                // This is a framework placeholder for the actual implementation
                Ok(Series::empty("result", &DataType::Binary))
            }
            _ => Err(DaftError::ValueError(
                "Invalid Lance KV expression type for batch_get operation".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let KVBatchGetWithConfigArgs {
            row_ids: _row_ids,
            kv_config: _,
            batch_size: _,
        } = args.try_into()?;

        let input_field = _row_ids.to_field(schema)?;
        Ok(Field::new(input_field.name, DataType::Binary))
    }
}

/// Arguments for KV Exists with Config function
#[derive(FunctionArgsDerive)]
struct KVExistsWithConfigArgs<T> {
    row_ids: T,

    #[arg(optional)]
    kv_config: Option<T>,
}

/// KV Exists with Config function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVExistsWithConfig;

#[typetag::serde]
impl ScalarUDF for KVExistsWithConfig {
    fn name(&self) -> &'static str {
        "kv_exists_with_config"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let KVExistsWithConfigArgs {
            row_ids: _row_ids,
            kv_config,
        } = args.try_into()?;

        // Extract parameters from KVConfig if provided
        let (uri_str, _columns_list, _batch_size, on_error_str) =
            if let Some(kv_config_series) = kv_config {
                let config = deserialize_kv_config_from_series(&kv_config_series)?;
                extract_lance_params_from_config(&config)?
            } else {
                // Fallback to default values if no config provided
                ("dummy_uri".to_string(), None, 1000, "raise".to_string())
            };

        let io_config_opt = None; // TODO: Extract from KVConfig when IOConfig is supported

        // Create Lance KV expression with extracted parameters
        let lance_expr = LanceKVExpr::exists(uri_str, on_error_str, io_config_opt);

        // Directly call the Lance implementation
        match &lance_expr {
            LanceKVExpr::Exists {
                uri: _uri,
                on_error: _on_error,
                io_config: _io_config,
            } => {
                // TODO: Implement Lance KV exists operation
                // This is a framework placeholder for the actual implementation
                Ok(Series::empty("result", &DataType::Boolean))
            }
            _ => Err(DaftError::ValueError(
                "Invalid Lance KV expression type for exists operation".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let KVExistsWithConfigArgs {
            row_ids: _row_ids,
            kv_config: _,
        } = args.try_into()?;

        let input_field = _row_ids.to_field(schema)?;
        Ok(Field::new(input_field.name, DataType::Boolean))
    }
}

/// Convenience functions for creating ScalarUDF expressions
use crate::functions::scalar::ScalarFn;

/// Create a kv_get_with_config ScalarUDF expression
pub fn kv_get_with_config(row_ids: ExprRef, kv_config: ExprRef) -> ExprRef {
    ScalarFn::builtin(KVGetWithConfig, vec![row_ids, kv_config]).into()
}

/// Create a kv_batch_get_with_config ScalarUDF expression
pub fn kv_batch_get_with_config(
    row_ids: ExprRef,
    kv_config: ExprRef,
    batch_size: ExprRef,
) -> ExprRef {
    ScalarFn::builtin(KVBatchGetWithConfig, vec![row_ids, kv_config, batch_size]).into()
}

/// Create a kv_exists_with_config ScalarUDF expression
pub fn kv_exists_with_config(row_ids: ExprRef, kv_config: ExprRef) -> ExprRef {
    ScalarFn::builtin(KVExistsWithConfig, vec![row_ids, kv_config]).into()
}

/// KV Config creation function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKVConfig;

#[typetag::serde]
impl ScalarUDF for CreateKVConfig {
    fn name(&self) -> &'static str {
        "create_kv_config"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        // For now, return a simple mock implementation
        // TODO: Implement proper KV config creation when Series construction API is clarified
        let _inputs = args.into_inner();

        // Create a simple binary series with serialized config
        let kv_config = KVConfig::new().with_lance(LanceConfig::new("dummy_uri".to_string()));
        let _serialized = serde_json::to_vec(&kv_config)
            .map_err(|e| DaftError::ValueError(format!("Failed to serialize KVConfig: {}", e)))?;

        // Return a simple binary series
        Ok(Series::empty("kv_config", &DataType::Binary))
    }

    fn get_return_field(
        &self,
        _args: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        // Return a struct type representing KVConfig
        Ok(Field::new(
            "kv_config",
            DataType::Struct(vec![
                Field::new("backend", DataType::Utf8),
                Field::new("config", DataType::Binary),
            ]),
        ))
    }
}
