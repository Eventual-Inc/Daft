use std::str::FromStr;

use arrow::{array::*, record_batch::RecordBatch};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};

use crate::sql::{
    errors::{SqlError, SqlResult},
    schema::{create_table_sqlite, drop_table_sqlite, table_exists_sqlite},
    type_mapping::SqlDialect,
};

pub async fn write_record_batch(
    connection_string: &str,
    table_name: &str,
    record_batch: &RecordBatch,
    mode: &str,
) -> SqlResult<(u64, u64)> {
    let dialect = SqlDialect::from_connection_string(connection_string)?;

    // For now, only support SQLite
    if dialect != SqlDialect::SQLite {
        return Err(SqlError::Database(
            "Currently only SQLite is supported. PostgreSQL and MySQL support coming soon."
                .to_string(),
        ));
    }

    let options = SqliteConnectOptions::from_str(connection_string)
        .map_err(|e| SqlError::Connection(e.to_string()))?;
    let pool = SqlitePool::connect_with(options)
        .await
        .map_err(|e| SqlError::Connection(e.to_string()))?;

    let table_exists_flag = table_exists_sqlite(&pool, table_name).await?;

    // Handle write modes
    match mode {
        "create" => {
            if table_exists_flag {
                return Err(SqlError::Database(format!(
                    "Table {} already exists in 'create' mode",
                    table_name
                )));
            }
            create_table_sqlite(&pool, table_name, &record_batch.schema()).await?;
        }
        "replace" => {
            if table_exists_flag {
                drop_table_sqlite(&pool, table_name).await?;
            }
            create_table_sqlite(&pool, table_name, &record_batch.schema()).await?;
        }
        "append" => {
            if !table_exists_flag {
                return Err(SqlError::Database(format!(
                    "Table {} does not exist in 'append' mode",
                    table_name
                )));
            }
        }
        _ => return Err(SqlError::Database(format!("Unknown write mode: {}", mode))),
    }

    // Insert data
    let rows_written = insert_record_batch_sqlite(&pool, table_name, record_batch).await?;
    let bytes_written = estimate_bytes(record_batch);

    Ok((rows_written as u64, bytes_written as u64))
}

async fn insert_record_batch_sqlite(
    pool: &SqlitePool,
    table_name: &str,
    record_batch: &RecordBatch,
) -> SqlResult<usize> {
    use sqlx::Row;

    let schema = record_batch.schema();
    let num_rows = record_batch.num_rows();

    if num_rows == 0 {
        return Ok(0);
    }

    // Build column names
    let column_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name().replace("\"", "\"\"")))
        .collect();
    let columns_str = column_names.join(", ");

    // Build INSERT statement row by row
    let mut total_inserted = 0;

    for row_idx in 0..num_rows {
        let mut values = Vec::new();

        for col_idx in 0..schema.fields().len() {
            let column = record_batch.column(col_idx);
            let value_str = format_value_sqlite(column, row_idx)?;
            values.push(value_str);
        }

        let values_str = values.join(", ");
        let insert_stmt = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            format!("\"{}\"", table_name.replace("\"", "\"\"")),
            columns_str,
            values_str
        );

        sqlx::query(&insert_stmt)
            .execute(pool)
            .await
            .map_err(|e| SqlError::Database(format!("Failed to insert row: {}", e)))?;

        total_inserted += 1;
    }

    Ok(total_inserted)
}

fn format_value_sqlite(column: &arrow::array::ArrayRef, row_idx: usize) -> SqlResult<String> {
    use arrow::array::*;

    if column.is_null(row_idx) {
        return Ok("NULL".to_string());
    }

    match column.data_type() {
        arrow::datatypes::DataType::Null => Ok("NULL".to_string()),
        arrow::datatypes::DataType::Boolean => {
            if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                Ok(if arr.value(row_idx) { "true" } else { "false" }.to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast Boolean array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::Int8 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int8Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast Int8 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::Int16 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int16Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast Int16 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::Int32 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast Int32 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::Int64 => {
            if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast Int64 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::UInt8 => {
            if let Some(arr) = column.as_any().downcast_ref::<UInt8Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast UInt8 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::UInt16 => {
            if let Some(arr) = column.as_any().downcast_ref::<UInt16Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast UInt16 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::UInt32 => {
            if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast UInt32 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::UInt64 => {
            if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast UInt64 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::Float32 => {
            if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast Float32 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::Float64 => {
            if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                Ok(arr.value(row_idx).to_string())
            } else {
                Err(SqlError::TypeConversion(
                    "Failed to downcast Float64 array".to_string(),
                ))
            }
        }
        arrow::datatypes::DataType::Utf8 => {
            if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                let val = arr.value(row_idx);
                Ok(format!("'{}'", val.replace("'", "''")))
            } else {
                Err(SqlError::TypeConversion(format!(
                    "Failed to downcast Utf8 array"
                )))
            }
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
                let val = arr.value(row_idx);
                Ok(format!("'{}'", val.replace("'", "''")))
            } else {
                Err(SqlError::TypeConversion(format!(
                    "Failed to downcast LargeUtf8 array"
                )))
            }
        }
        arrow::datatypes::DataType::Binary => {
            if let Some(arr) = column.as_any().downcast_ref::<BinaryArray>() {
                let val = arr.value(row_idx);
                let hex_str = hex::encode(val);
                Ok(format!("X'{}'", hex_str))
            } else {
                Err(SqlError::TypeConversion(format!(
                    "Failed to downcast Binary array"
                )))
            }
        }
        arrow::datatypes::DataType::LargeBinary => {
            if let Some(arr) = column.as_any().downcast_ref::<LargeBinaryArray>() {
                let val = arr.value(row_idx);
                let hex_str = hex::encode(val);
                Ok(format!("X'{}'", hex_str))
            } else {
                Err(SqlError::TypeConversion(format!(
                    "Failed to downcast LargeBinary array"
                )))
            }
        }
        _ => Err(SqlError::TypeConversion(format!(
            "Unsupported data type for formatting: {:?}",
            column.data_type()
        ))),
    }
}

fn estimate_bytes(record_batch: &RecordBatch) -> usize {
    // Simple estimation based on memory size
    record_batch.get_array_memory_size()
}
