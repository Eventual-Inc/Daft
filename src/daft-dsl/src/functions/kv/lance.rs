use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_io::IOConfig;
use serde::{Deserialize, Serialize};

use super::cache::{get_global_cache, init_global_cache, LanceCacheConfig};
use crate::{
    functions::{FunctionEvaluator, FunctionExpr},
    ExprRef,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum LanceKVExpr {
    Get {
        uri: String,
        columns: Option<Vec<String>>,
        on_error: String,
        io_config: Option<IOConfig>,
    },
    BatchGet {
        uri: String,
        columns: Option<Vec<String>>,
        batch_size: usize,
        on_error: String,
        io_config: Option<IOConfig>,
    },
    Exists {
        uri: String,
        on_error: String,
        io_config: Option<IOConfig>,
    },
}

impl LanceKVExpr {
    pub fn get(
        uri: String,
        columns: Option<Vec<String>>,
        on_error: String,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self::Get {
            uri,
            columns,
            on_error,
            io_config,
        }
    }

    pub fn batch_get(
        uri: String,
        columns: Option<Vec<String>>,
        batch_size: usize,
        on_error: String,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self::BatchGet {
            uri,
            columns,
            batch_size,
            on_error,
            io_config,
        }
    }

    pub fn exists(uri: String, on_error: String, io_config: Option<IOConfig>) -> Self {
        Self::Exists {
            uri,
            on_error,
            io_config,
        }
    }
}

impl FunctionEvaluator for LanceKVExpr {
    fn fn_name(&self) -> &'static str {
        match self {
            Self::Get { .. } => "kv_get",
            Self::BatchGet { .. } => "kv_batch_get",
            Self::Exists { .. } => "kv_exists",
        }
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        schema: &Schema,
        _expr: &FunctionExpr,
    ) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                match self {
                    Self::Get { .. } | Self::BatchGet { .. } => {
                        // Return binary data type for get/take operations
                        Ok(Field::new(input_field.name, DataType::Binary))
                    }
                    Self::Exists { .. } => {
                        // Return boolean for exists operation
                        Ok(Field::new(input_field.name, DataType::Boolean))
                    }
                }
            }
            _ => Err(DaftError::ValueError(
                "Lance KV operations expect exactly one input (row_id column)".to_string(),
            )),
        }
    }

    fn evaluate(&self, inputs: &[Series], _expr: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [row_ids] => match self {
                Self::Get {
                    uri,
                    columns,
                    on_error,
                    io_config,
                } => {
                    lance_kv_get_impl(row_ids, uri, columns.as_ref(), on_error, io_config.as_ref())
                }
                Self::BatchGet {
                    uri,
                    columns,
                    batch_size,
                    on_error,
                    io_config,
                } => lance_kv_batch_get_impl(
                    row_ids,
                    uri,
                    columns.as_ref(),
                    *batch_size,
                    on_error,
                    io_config.as_ref(),
                ),
                Self::Exists {
                    uri,
                    on_error,
                    io_config,
                } => lance_kv_exists_impl(row_ids, uri, on_error, io_config.as_ref()),
            },
            _ => Err(DaftError::ValueError(
                "Lance KV operations expect exactly one input (row_id column)".to_string(),
            )),
        }
    }
}

/// Core implementation for Lance KV get operation
pub fn lance_kv_get_impl(
    row_ids: &Series,
    uri: &str,
    columns: Option<&Vec<String>>,
    on_error: &str,
    io_config: Option<&IOConfig>,
) -> DaftResult<Series> {
    use daft_core::array::ops::as_arrow::AsArrow;

    // Convert row_ids to indices for Lance take operation
    let indices = match row_ids.data_type() {
        DataType::UInt64 => {
            let array = row_ids.u64()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.map(|x| *x as usize))
                .collect::<Vec<_>>()
        }
        DataType::Int64 => {
            let array = row_ids.i64()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.and_then(|x| if *x >= 0 { Some(*x as usize) } else { None }))
                .collect::<Vec<_>>()
        }
        DataType::Int32 => {
            let array = row_ids.i32()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.and_then(|x| if *x >= 0 { Some(*x as usize) } else { None }))
                .collect::<Vec<_>>()
        }
        DataType::List(inner_type) => match inner_type.as_ref() {
            DataType::Int64 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.i64()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            if *x >= 0 {
                                all_indices.push(*x as usize);
                            }
                        }
                    }
                }
                all_indices
            }
            DataType::Int32 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.i32()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            if *x >= 0 {
                                all_indices.push(*x as usize);
                            }
                        }
                    }
                }
                all_indices
            }
            DataType::UInt64 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.u64()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            all_indices.push(*x as usize);
                        }
                    }
                }
                all_indices
            }
            _ => {
                return Err(DaftError::ValueError(
                    "List row IDs must contain integer types (UInt64, Int64, or Int32)".to_string(),
                ))
            }
        },
        _ => {
            return Err(DaftError::ValueError(
                "Row IDs must be integer type (UInt64, Int64, or Int32) or List of these types"
                    .to_string(),
            ))
        }
    };

    if indices.is_empty() {
        // Return empty binary series
        return Ok(Series::empty("result", &DataType::Binary));
    }

    // Perform Lance take operation
    match lance_take_rows(uri, &indices, columns, io_config) {
        Ok(_binary_data) => {
            // Create binary series from the serialized data
            use std::sync::Arc;

            // Create a large binary array with the serialized data
            let values = [_binary_data];
            let binary_array = arrow2::array::BinaryArray::<i64>::from_iter_values(
                values.iter().map(|v| v.as_slice()),
            );

            // Create field for the binary data using input field name
            let field = Arc::new(Field::new(row_ids.name(), DataType::Binary));

            // Convert to Daft Series using from_arrow
            Series::from_arrow(field, Box::new(binary_array))
        }
        Err(e) => {
            if on_error == "null" {
                // Return null values
                let _nulls: Vec<Option<Vec<u8>>> = vec![None; row_ids.len()];
                // For now, return empty binary series as placeholder
                Ok(Series::empty("result", &DataType::Binary))
            } else {
                Err(e)
            }
        }
    }
}

/// Core implementation for Lance KV batch_get operation with batch processing
pub fn lance_kv_batch_get_impl(
    row_ids: &Series,
    uri: &str,
    columns: Option<&Vec<String>>,
    batch_size: usize,
    on_error: &str,
    io_config: Option<&IOConfig>,
) -> DaftResult<Series> {
    use daft_core::array::ops::as_arrow::AsArrow;

    // Convert row_ids to indices
    let indices = match row_ids.data_type() {
        DataType::UInt64 => {
            let array = row_ids.u64()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.map(|x| *x as usize))
                .collect::<Vec<_>>()
        }
        DataType::Int64 => {
            let array = row_ids.i64()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.and_then(|x| if *x >= 0 { Some(*x as usize) } else { None }))
                .collect::<Vec<_>>()
        }
        DataType::Int32 => {
            let array = row_ids.i32()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.and_then(|x| if *x >= 0 { Some(*x as usize) } else { None }))
                .collect::<Vec<_>>()
        }
        DataType::List(inner_type) => match inner_type.as_ref() {
            DataType::Int64 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.i64()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            if *x >= 0 {
                                all_indices.push(*x as usize);
                            }
                        }
                    }
                }
                all_indices
            }
            DataType::Int32 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.i32()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            if *x >= 0 {
                                all_indices.push(*x as usize);
                            }
                        }
                    }
                }
                all_indices
            }
            DataType::UInt64 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.u64()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            all_indices.push(*x as usize);
                        }
                    }
                }
                all_indices
            }
            _ => {
                return Err(DaftError::ValueError(
                    "List row IDs must contain integer types (UInt64, Int64, or Int32)".to_string(),
                ))
            }
        },
        _ => {
            return Err(DaftError::ValueError(
                "Row IDs must be integer type (UInt64, Int64, or Int32) or List of these types"
                    .to_string(),
            ))
        }
    };

    if indices.is_empty() {
        return Ok(Series::empty("result", &DataType::Binary));
    }

    // Process in batches for better performance
    let mut results = Vec::new();
    for chunk in indices.chunks(batch_size) {
        match lance_take_rows(uri, chunk, columns, io_config) {
            Ok(_binary_data) => {
                // Convert binary data to Series using proper Daft API
                // For now, use mock data until Series creation is fixed
                results.push(Some(b"mock_batch_data".to_vec()));
            }
            Err(e) => {
                if on_error == "null" {
                    // Add null values for this batch
                    results.extend(vec![None; chunk.len()]);
                } else {
                    return Err(e);
                }
            }
        }
    }

    // Create binary series from collected results
    use std::sync::Arc;

    use arrow2::array::BinaryArray;

    // Convert results to binary array
    let binary_values: Vec<Vec<u8>> = results
        .into_iter()
        .map(|opt| opt.unwrap_or_else(|| b"null".to_vec()))
        .collect();

    let binary_array =
        BinaryArray::<i64>::from_iter_values(binary_values.iter().map(|v| v.as_slice()));

    // Create field for the binary data using input field name
    let field = Arc::new(Field::new(row_ids.name(), DataType::Binary));

    // Convert to Daft Series using from_arrow
    Series::from_arrow(field, Box::new(binary_array))
}

/// Core implementation for Lance KV exists operation
pub fn lance_kv_exists_impl(
    row_ids: &Series,
    uri: &str,
    on_error: &str,
    io_config: Option<&IOConfig>,
) -> DaftResult<Series> {
    use daft_core::array::ops::as_arrow::AsArrow;

    // Convert row_ids to indices
    let indices = match row_ids.data_type() {
        DataType::UInt64 => {
            let array = row_ids.u64()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.map(|x| *x as usize))
                .collect::<Vec<_>>()
        }
        DataType::Int64 => {
            let array = row_ids.i64()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.and_then(|x| if *x >= 0 { Some(*x as usize) } else { None }))
                .collect::<Vec<_>>()
        }
        DataType::Int32 => {
            let array = row_ids.i32()?;
            array
                .as_arrow()
                .iter()
                .filter_map(|v| v.and_then(|x| if *x >= 0 { Some(*x as usize) } else { None }))
                .collect::<Vec<_>>()
        }
        DataType::List(inner_type) => match inner_type.as_ref() {
            DataType::Int64 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.i64()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            if *x >= 0 {
                                all_indices.push(*x as usize);
                            }
                        }
                    }
                }
                all_indices
            }
            DataType::Int32 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.i32()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            if *x >= 0 {
                                all_indices.push(*x as usize);
                            }
                        }
                    }
                }
                all_indices
            }
            DataType::UInt64 => {
                let array = row_ids.list()?;
                let mut all_indices = Vec::new();
                for i in 0..array.len() {
                    if let Some(list_series) = array.get(i) {
                        let int_array = list_series.u64()?;
                        for x in int_array.as_arrow().iter().flatten() {
                            all_indices.push(*x as usize);
                        }
                    }
                }
                all_indices
            }
            _ => {
                return Err(DaftError::ValueError(
                    "List row IDs must contain integer types (UInt64, Int64, or Int32)".to_string(),
                ))
            }
        },
        _ => {
            return Err(DaftError::ValueError(
                "Row IDs must be integer type (UInt64, Int64, or Int32) or List of these types"
                    .to_string(),
            ))
        }
    };

    if indices.is_empty() {
        return Ok(Series::empty("result", &DataType::Boolean));
    }

    // Check existence using Lance dataset metadata
    match lance_check_rows_exist(uri, &indices, io_config) {
        Ok(exists_flags) => {
            // Create boolean series from the existence flags
            use std::sync::Arc;

            use arrow2::array::BooleanArray;

            // Create a boolean array with the existence flags
            let boolean_array = BooleanArray::from_slice(exists_flags);

            // Create field for the boolean data using input field name
            let field = Arc::new(Field::new(row_ids.name(), DataType::Boolean));

            // Convert to Daft Series using from_arrow
            Series::from_arrow(field, Box::new(boolean_array))
        }
        Err(e) => {
            if on_error == "null" {
                // Return null values
                let _nulls: Vec<Option<Vec<u8>>> = vec![None; row_ids.len()];
                // For now, return empty boolean series as placeholder
                Ok(Series::empty("result", &DataType::Boolean))
            } else {
                Err(e)
            }
        }
    }
}

/// Lance take_rows implementation using Lance Rust SDK
fn lance_take_rows(
    uri: &str,
    indices: &[usize],
    columns: Option<&Vec<String>>,
    _io_config: Option<&IOConfig>,
) -> DaftResult<Vec<u8>> {
    // Try to use current runtime handle if available
    if let Ok(_handle) = tokio::runtime::Handle::try_current() {
        // We're already in a runtime context, use spawn_blocking to avoid nested runtime issues
        let uri = uri.to_string();
        let indices = indices.to_vec();
        let columns = columns.cloned();

        // Use spawn_blocking to run async operations without creating nested runtime
        let result = std::thread::spawn(move || {
            // Create a new runtime in this dedicated thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    DaftError::External(format!("Failed to create Tokio runtime: {}", e).into())
                })?;

            rt.block_on(lance_take_rows_async(&uri, &indices, columns.as_ref()))
        })
        .join()
        .map_err(|_| DaftError::External("Failed to join Lance async thread".into()))??;

        Ok(result)
    } else {
        // No current runtime, create a new one
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            DaftError::External(format!("Failed to create Tokio runtime: {}", e).into())
        })?;

        let result = rt.block_on(lance_take_rows_async(uri, indices, columns));

        result
    }
}

/// Async implementation of lance_take_rows
async fn lance_take_rows_async(
    uri: &str,
    indices: &[usize],
    columns: Option<&Vec<String>>,
) -> DaftResult<Vec<u8>> {
    // Get dataset from cache or open it
    let cache = get_global_cache();
    let dataset = cache.get_or_open(uri).await?;

    // Convert indices to row IDs (u64)
    let row_ids: Vec<u64> = indices.iter().map(|&i| i as u64).collect();

    // Build take operation with proper projection
    let projection = if let Some(cols) = columns {
        dataset.schema().project(cols).map_err(|e| {
            DaftError::External(format!("Failed to create projection: {}", e).into())
        })?
    } else {
        dataset.schema().clone()
    };

    // Execute take operation
    let record_batch = dataset.take(&row_ids, projection).await.map_err(|e| {
        DaftError::External(format!("Failed to execute Lance take operation: {}", e).into())
    })?;

    // Serialize to binary format
    serialize_arrow_table_to_binary(&record_batch)
}

/// Check if rows exist in Lance dataset
fn lance_check_rows_exist(
    uri: &str,
    indices: &[usize],
    _io_config: Option<&IOConfig>,
) -> DaftResult<Vec<bool>> {
    // Try to use current runtime handle if available
    if let Ok(_handle) = tokio::runtime::Handle::try_current() {
        // We're already in a runtime context, use spawn_blocking to avoid nested runtime issues
        let uri = uri.to_string();
        let indices = indices.to_vec();

        // Use spawn_blocking to run async operations without creating nested runtime
        let result = std::thread::spawn(move || {
            // Create a new runtime in this dedicated thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    DaftError::External(format!("Failed to create Tokio runtime: {}", e).into())
                })?;

            rt.block_on(lance_check_rows_exist_async(&uri, &indices))
        })
        .join()
        .map_err(|_| DaftError::External("Failed to join Lance async thread".into()))??;

        Ok(result)
    } else {
        // No current runtime, create a new one
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            DaftError::External(format!("Failed to create Tokio runtime: {}", e).into())
        })?;

        rt.block_on(lance_check_rows_exist_async(uri, indices))
    }
}

/// Async implementation of lance_check_rows_exist
async fn lance_check_rows_exist_async(uri: &str, indices: &[usize]) -> DaftResult<Vec<bool>> {
    // Get dataset from cache or open it
    let cache = get_global_cache();
    let dataset = cache.get_or_open(uri).await?;

    // Get total number of rows in the dataset
    let total_rows = dataset.count_rows(None).await.map_err(|e| {
        DaftError::External(format!("Failed to count rows in Lance dataset: {}", e).into())
    })?;

    // Check if each index exists (is within bounds)
    let exists_flags: Vec<bool> = indices
        .iter()
        .map(|&i| (i as u64) < (total_rows as u64))
        .collect();

    Ok(exists_flags)
}

/// Serialize Arrow RecordBatch to binary format using Arrow IPC
fn serialize_arrow_table_to_binary(
    record_batch: &arrow::record_batch::RecordBatch,
) -> DaftResult<Vec<u8>> {
    use std::io::Cursor;

    use arrow_ipc::writer::StreamWriter;

    // Create a buffer to write the serialized data
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);

        // Create Arrow IPC StreamWriter
        let mut writer =
            StreamWriter::try_new(cursor, record_batch.schema().as_ref()).map_err(|e| {
                DaftError::External(format!("Failed to create Arrow IPC writer: {}", e).into())
            })?;

        // Write the record batch
        writer.write(record_batch).map_err(|e| {
            DaftError::External(format!("Failed to write record batch to Arrow IPC: {}", e).into())
        })?;

        // Finish writing
        writer.finish().map_err(|e| {
            DaftError::External(format!("Failed to finish Arrow IPC writing: {}", e).into())
        })?;
    }

    Ok(buffer)
}

/// Initialize the Lance dataset cache with custom configuration
pub fn init_lance_cache(max_size: usize, ttl_seconds: u64) {
    let config = LanceCacheConfig::new(max_size, ttl_seconds);
    init_global_cache(config);
}

/// Initialize the Lance dataset cache with default configuration
pub fn init_lance_cache_default() {
    let config = LanceCacheConfig::default();
    init_global_cache(config);
}

/// Disable the Lance dataset cache
pub fn disable_lance_cache() {
    let config = LanceCacheConfig::disabled();
    init_global_cache(config);
}

/// Get cache statistics
pub fn get_lance_cache_stats() -> super::cache::CacheStats {
    get_global_cache().stats()
}

/// Clear the Lance dataset cache
pub fn clear_lance_cache() {
    get_global_cache().clear();
}

/// Cleanup expired entries from the cache
pub fn cleanup_lance_cache() {
    get_global_cache().cleanup_expired();
}
