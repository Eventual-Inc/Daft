use arrow::datatypes::SchemaRef;
use sqlx::{AnyPool, Row, sqlite::SqlitePool};

use crate::sql::{
    errors::{SqlError, SqlResult},
    type_mapping::{SqlDialect, arrow_to_sql_type},
};

pub async fn table_exists(
    pool: &AnyPool,
    table_name: &str,
    connection_string: &str,
) -> SqlResult<bool> {
    let dialect = detect_dialect(connection_string)?;

    let query = match dialect {
        SqlDialect::PostgreSQL => {
            format!(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{}')",
                escape_sql_identifier(table_name)
            )
        }
        SqlDialect::SQLite => {
            format!(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='{}')",
                escape_sql_identifier(table_name)
            )
        }
        SqlDialect::MySQL => {
            format!(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{}')",
                escape_sql_identifier(table_name)
            )
        }
    };

    let result = sqlx::query(&query)
        .fetch_one(pool)
        .await
        .map_err(|e| SqlError::Database(e.to_string()))?;

    let exists: bool = result.get(0);
    Ok(exists)
}

pub async fn create_table(
    pool: &AnyPool,
    table_name: &str,
    schema: &SchemaRef,
    connection_string: &str,
) -> SqlResult<()> {
    let dialect = detect_dialect(connection_string)?;

    let mut columns = Vec::new();
    for field in schema.fields() {
        let sql_type = arrow_to_sql_type(&field.data_type(), dialect)?;
        let col_def = format!("{} {}", escape_sql_identifier(&field.name()), sql_type);
        columns.push(col_def);
    }

    let columns_str = columns.join(", ");
    let create_stmt = format!(
        "CREATE TABLE {} ({})",
        escape_sql_identifier(table_name),
        columns_str
    );

    sqlx::query(&create_stmt)
        .execute(pool)
        .await
        .map_err(|e| SqlError::Database(e.to_string()))?;

    Ok(())
}

pub async fn drop_table(pool: &AnyPool, table_name: &str) -> SqlResult<()> {
    let drop_stmt = format!("DROP TABLE IF EXISTS {}", escape_sql_identifier(table_name));

    sqlx::query(&drop_stmt)
        .execute(pool)
        .await
        .map_err(|e| SqlError::Database(e.to_string()))?;

    Ok(())
}

pub fn detect_dialect(connection_string: &str) -> SqlResult<SqlDialect> {
    SqlDialect::from_connection_string(connection_string)
}

fn escape_sql_identifier(identifier: &str) -> String {
    // Simple escaping - can be enhanced
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}

// SQLite-specific functions
pub async fn table_exists_sqlite(pool: &SqlitePool, table_name: &str) -> SqlResult<bool> {
    let query = format!(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='{}')",
        escape_sql_identifier(table_name)
    );

    let result = sqlx::query(&query)
        .fetch_one(pool)
        .await
        .map_err(|e| SqlError::Database(e.to_string()))?;

    let exists: bool = result.get(0);
    Ok(exists)
}

pub async fn create_table_sqlite(
    pool: &SqlitePool,
    table_name: &str,
    schema: &SchemaRef,
) -> SqlResult<()> {
    let mut columns = Vec::new();
    for field in schema.fields() {
        let sql_type = arrow_to_sql_type(&field.data_type(), SqlDialect::SQLite)?;
        let col_def = format!("{} {}", escape_sql_identifier(&field.name()), sql_type);
        columns.push(col_def);
    }

    let columns_str = columns.join(", ");
    let create_stmt = format!(
        "CREATE TABLE {} ({})",
        escape_sql_identifier(table_name),
        columns_str
    );

    sqlx::query(&create_stmt)
        .execute(pool)
        .await
        .map_err(|e| SqlError::Database(e.to_string()))?;

    Ok(())
}

pub async fn drop_table_sqlite(pool: &SqlitePool, table_name: &str) -> SqlResult<()> {
    let drop_stmt = format!("DROP TABLE IF EXISTS {}", escape_sql_identifier(table_name));

    sqlx::query(&drop_stmt)
        .execute(pool)
        .await
        .map_err(|e| SqlError::Database(e.to_string()))?;

    Ok(())
}
