use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use serde::{Deserialize, Serialize};

use super::{lance::LanceKVExpr, KVConfig, LanceConfig};
use crate::{
    functions::{
        function_args::{FunctionArgs, FunctionArgs as FunctionArgsDerive},
        scalar::ScalarUDF,
    },
    ExprRef,
};

/// Helper function to deserialize KVConfig from Series
fn deserialize_kv_config_from_series(kv_config_series: &Series) -> DaftResult<KVConfig> {
    // Write debug info to file to bypass stderr issues
    std::fs::write(
        "/tmp/rust_debug.log",
        "DEBUG: deserialize_kv_config_from_series called\n",
    )
    .ok();

    let debug_msg = format!(
        "DEBUG: Series length: {}, data type: {:?}\n",
        kv_config_series.len(),
        kv_config_series.data_type()
    );
    std::fs::write("/tmp/rust_debug.log", debug_msg).ok();

    // Try to extract the KVConfig from the series
    // The series should contain serialized KVConfig data
    if kv_config_series.len() != 1 {
        let err_msg = format!(
            "KVConfig series must contain exactly one element, got {}",
            kv_config_series.len()
        );
        std::fs::write(
            "/tmp/rust_debug.log",
            format!("DEBUG: Length check failed: {}\n", err_msg),
        )
        .ok();
        return Err(DaftError::ValueError(err_msg));
    }

    // Try to get the binary data from the series
    std::fs::write(
        "/tmp/rust_debug.log",
        "DEBUG: Attempting to extract binary data from series\n",
    )
    .ok();
    let binary_array = kv_config_series.binary().map_err(|e| {
        std::fs::write(
            "/tmp/rust_debug.log",
            format!("DEBUG: Failed to get binary array: {}\n", e),
        )
        .ok();
        e
    })?;

    std::fs::write(
        "/tmp/rust_debug.log",
        "DEBUG: Binary array extracted successfully\n",
    )
    .ok();
    let binary_data = binary_array.get(0).ok_or_else(|| {
        std::fs::write(
            "/tmp/rust_debug.log",
            "DEBUG: No data at index 0 in binary array\n",
        )
        .ok();
        DaftError::ValueError("KVConfig series contains no data".to_string())
    })?;

    let debug_msg = format!("DEBUG: Binary data length: {}\n", binary_data.len());
    std::fs::write("/tmp/rust_debug.log", debug_msg).ok();

    // Try to convert to string for debugging
    if let Ok(json_str) = std::str::from_utf8(binary_data) {
        let debug_msg = format!("DEBUG: JSON string: {}\n", json_str);
        std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
    } else {
        std::fs::write(
            "/tmp/rust_debug.log",
            "DEBUG: Binary data is not valid UTF-8\n",
        )
        .ok();
    }

    // Deserialize the JSON data
    std::fs::write(
        "/tmp/rust_debug.log",
        "DEBUG: Attempting JSON deserialization\n",
    )
    .ok();
    let kv_config: KVConfig = serde_json::from_slice(binary_data).map_err(|e| {
        let debug_msg = format!("DEBUG: JSON deserialization failed: {}\n", e);
        std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
        DaftError::ValueError(format!("Failed to deserialize KVConfig: {}", e))
    })?;

    let debug_msg = format!(
        "DEBUG: KVConfig deserialized successfully: {:?}\n",
        kv_config
    );
    std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
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

/// Arguments for Lance KV Get function
#[derive(FunctionArgsDerive)]
struct LanceKVGetArgs<T> {
    row_ids: T,

    #[arg(optional)]
    uri: Option<T>,

    #[arg(optional)]
    #[allow(dead_code)]
    columns: Option<T>,

    #[arg(optional)]
    on_error: Option<T>,

    #[arg(optional)]
    #[allow(dead_code)]
    io_config: Option<T>,
}

/// Lance KV Get function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceKVGet;

#[typetag::serde]
impl ScalarUDF for LanceKVGet {
    fn name(&self) -> &'static str {
        "kv_get"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        std::fs::write(
            "/tmp/rust_debug.log",
            "DEBUG: LanceKVGet::call was invoked (legacy function)\n",
        )
        .ok();

        let LanceKVGetArgs {
            row_ids,
            uri,
            columns: _,
            on_error,
            io_config: _,
        } = args.try_into()?;

        // Extract URI from the series
        let uri_str = if let Some(uri_series) = uri {
            // Extract string value from the URI series
            if uri_series.len() == 1 {
                let uri_array = uri_series.utf8()?;
                uri_array.get(0).unwrap_or("dummy_uri").to_string()
            } else {
                "dummy_uri".to_string()
            }
        } else {
            "dummy_uri".to_string()
        };

        // Extract columns (for now, we'll ignore this and use None)
        let columns_list = None; // TODO: Parse columns from series

        // Extract on_error setting
        let on_error_str = if let Some(on_error_series) = on_error {
            if on_error_series.len() == 1 {
                let on_error_array = on_error_series.utf8()?;
                on_error_array.get(0).unwrap_or("raise").to_string()
            } else {
                "raise".to_string()
            }
        } else {
            "raise".to_string()
        };

        // Extract IO config (for now, we'll ignore this and use None)
        let io_config_opt = None; // TODO: Parse IOConfig from series

        // Create Lance KV expression with extracted parameters
        let lance_expr = LanceKVExpr::get(uri_str, columns_list, on_error_str, io_config_opt);

        // Directly call the Lance implementation without recursive evaluation
        match &lance_expr {
            LanceKVExpr::Get {
                uri,
                columns,
                on_error,
                io_config,
            } => super::lance::lance_kv_get_impl(
                &row_ids,
                uri,
                columns.as_ref(),
                on_error,
                io_config.as_ref(),
            ),
            _ => Err(DaftError::ValueError(
                "Invalid Lance KV expression type for get operation".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let LanceKVGetArgs {
            row_ids,
            uri: _,
            columns: _,
            on_error: _,
            io_config: _,
        } = args.try_into()?;

        let input_field = row_ids.to_field(schema)?;
        Ok(Field::new(input_field.name, DataType::Binary))
    }
}

/// Lance KV BatchGet function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceKVBatchGet;

#[typetag::serde]
impl ScalarUDF for LanceKVBatchGet {
    fn name(&self) -> &'static str {
        "kv_batch_get"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let _inputs = args.into_inner();
        match _inputs.as_slice() {
            [row_ids] => {
                // For now, create a simple Lance KV expression
                // TODO: Extract proper arguments when FunctionArgs supports named parameters
                let lance_expr = LanceKVExpr::batch_get(
                    "dummy_uri".to_string(),
                    None,
                    1000,
                    "raise".to_string(),
                    None,
                );

                // Directly call the Lance implementation without recursive evaluation
                match &lance_expr {
                    LanceKVExpr::BatchGet {
                        uri,
                        columns,
                        batch_size,
                        on_error,
                        io_config,
                    } => super::lance::lance_kv_batch_get_impl(
                        row_ids,
                        uri,
                        columns.as_ref(),
                        *batch_size,
                        on_error,
                        io_config.as_ref(),
                    ),
                    _ => Err(DaftError::ValueError(
                        "Invalid Lance KV expression type for batch_get operation".to_string(),
                    )),
                }
            }
            _ => Err(DaftError::ValueError(
                "kv_batch_get expects exactly one input (row_id column)".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let _inputs = args.into_inner();
        match _inputs.as_slice() {
            [input] => {
                let input_field = input.to_field(schema)?;
                Ok(Field::new(input_field.name, DataType::Binary))
            }
            _ => Err(DaftError::ValueError(
                "kv_batch_get expects exactly one input (row_id column)".to_string(),
            )),
        }
    }
}

/// Lance KV Exists function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanceKVExists;

#[typetag::serde]
impl ScalarUDF for LanceKVExists {
    fn name(&self) -> &'static str {
        "kv_exists"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let _inputs = args.into_inner();
        match _inputs.as_slice() {
            [row_ids] => {
                // For now, create a simple Lance KV expression
                // TODO: Extract proper arguments when FunctionArgs supports named parameters
                let lance_expr =
                    LanceKVExpr::exists("dummy_uri".to_string(), "raise".to_string(), None);

                // Directly call the Lance implementation without recursive evaluation
                match &lance_expr {
                    LanceKVExpr::Exists {
                        uri,
                        on_error,
                        io_config,
                    } => super::lance::lance_kv_exists_impl(
                        row_ids,
                        uri,
                        on_error,
                        io_config.as_ref(),
                    ),
                    _ => Err(DaftError::ValueError(
                        "Invalid Lance KV expression type for exists operation".to_string(),
                    )),
                }
            }
            _ => Err(DaftError::ValueError(
                "kv_exists expects exactly one input (row_id column)".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let _inputs = args.into_inner();
        match _inputs.as_slice() {
            [input] => {
                let input_field = input.to_field(schema)?;
                Ok(Field::new(input_field.name, DataType::Boolean))
            }
            _ => Err(DaftError::ValueError(
                "kv_exists expects exactly one input (row_id column)".to_string(),
            )),
        }
    }
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

        let KVGetWithConfigArgs { row_ids, kv_config } = args.try_into()?;

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
                columns,
                on_error,
                io_config,
            } => {
                let debug_msg =
                    format!("DEBUG: About to call lance_kv_get_impl with URI: {}\n", uri);
                std::fs::write("/tmp/rust_debug.log", debug_msg).ok();
                super::lance::lance_kv_get_impl(
                    &row_ids,
                    uri,
                    columns.as_ref(),
                    on_error,
                    io_config.as_ref(),
                )
            }
            _ => Err(DaftError::ValueError(
                "Invalid Lance KV expression type for get operation".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let KVGetWithConfigArgs {
            row_ids,
            kv_config: _,
        } = args.try_into()?;

        let input_field = row_ids.to_field(schema)?;
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
            row_ids,
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
                uri,
                columns,
                batch_size,
                on_error,
                io_config,
            } => super::lance::lance_kv_batch_get_impl(
                &row_ids,
                uri,
                columns.as_ref(),
                *batch_size,
                on_error,
                io_config.as_ref(),
            ),
            _ => Err(DaftError::ValueError(
                "Invalid Lance KV expression type for batch_get operation".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let KVBatchGetWithConfigArgs {
            row_ids,
            kv_config: _,
            batch_size: _,
        } = args.try_into()?;

        let input_field = row_ids.to_field(schema)?;
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
        let KVExistsWithConfigArgs { row_ids, kv_config } = args.try_into()?;

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
                uri,
                on_error,
                io_config,
            } => super::lance::lance_kv_exists_impl(&row_ids, uri, on_error, io_config.as_ref()),
            _ => Err(DaftError::ValueError(
                "Invalid Lance KV expression type for exists operation".to_string(),
            )),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let KVExistsWithConfigArgs {
            row_ids,
            kv_config: _,
        } = args.try_into()?;

        let input_field = row_ids.to_field(schema)?;
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
