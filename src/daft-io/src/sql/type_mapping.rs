use arrow::datatypes::DataType;

use crate::sql::errors::{SqlError, SqlResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    PostgreSQL,
    SQLite,
    MySQL,
}

impl SqlDialect {
    pub fn from_connection_string(conn_str: &str) -> SqlResult<Self> {
        if conn_str.starts_with("postgresql://") || conn_str.starts_with("postgres://") {
            Ok(SqlDialect::PostgreSQL)
        } else if conn_str.starts_with("sqlite://") {
            Ok(SqlDialect::SQLite)
        } else if conn_str.starts_with("mysql://") || conn_str.starts_with("mariadb://") {
            Ok(SqlDialect::MySQL)
        } else {
            Err(SqlError::Connection(format!(
                "Unsupported database URL: {}",
                conn_str
            )))
        }
    }
}

pub fn arrow_to_sql_type(arrow_type: &DataType, dialect: SqlDialect) -> SqlResult<String> {
    match arrow_type {
        DataType::Null => Ok("TEXT".to_string()),
        DataType::Boolean => match dialect {
            SqlDialect::PostgreSQL => Ok("BOOLEAN".to_string()),
            SqlDialect::SQLite => Ok("INTEGER".to_string()),
            SqlDialect::MySQL => Ok("BOOLEAN".to_string()),
        },
        DataType::Int8 => Ok("SMALLINT".to_string()),
        DataType::Int16 => Ok("SMALLINT".to_string()),
        DataType::Int32 => Ok("INTEGER".to_string()),
        DataType::Int64 => Ok("BIGINT".to_string()),
        DataType::UInt8 => Ok("SMALLINT".to_string()),
        DataType::UInt16 => Ok("INTEGER".to_string()),
        DataType::UInt32 => Ok("BIGINT".to_string()),
        DataType::UInt64 => Ok("BIGINT".to_string()),
        DataType::Float16 => Ok("REAL".to_string()),
        DataType::Float32 => Ok("REAL".to_string()),
        DataType::Float64 => match dialect {
            SqlDialect::PostgreSQL => Ok("DOUBLE PRECISION".to_string()),
            _ => Ok("DOUBLE".to_string()),
        },
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Ok("DECIMAL".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 => Ok("TEXT".to_string()),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => match dialect {
            SqlDialect::PostgreSQL => Ok("BYTEA".to_string()),
            SqlDialect::MySQL => Ok("LONGBLOB".to_string()),
            SqlDialect::SQLite => Ok("BLOB".to_string()),
        },
        DataType::Date32 | DataType::Date64 => Ok("DATE".to_string()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("TIME".to_string()),
        DataType::Timestamp(_, _) => match dialect {
            SqlDialect::PostgreSQL => Ok("TIMESTAMP".to_string()),
            SqlDialect::MySQL => Ok("DATETIME".to_string()),
            SqlDialect::SQLite => Ok("TEXT".to_string()),
        },
        DataType::Duration(_) => Ok("BIGINT".to_string()),
        DataType::Interval(_) => Ok("TEXT".to_string()),
        _ => Err(SqlError::TypeConversion(format!(
            "Unsupported Arrow type: {:?}",
            arrow_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dialect_detection() {
        assert_eq!(
            SqlDialect::from_connection_string("postgresql://localhost/db").unwrap(),
            SqlDialect::PostgreSQL
        );
        assert_eq!(
            SqlDialect::from_connection_string("sqlite:///test.db").unwrap(),
            SqlDialect::SQLite
        );
        assert_eq!(
            SqlDialect::from_connection_string("mysql://localhost/db").unwrap(),
            SqlDialect::MySQL
        );
    }

    #[test]
    fn test_arrow_to_sql_type_mapping() {
        let dialect = SqlDialect::PostgreSQL;
        assert_eq!(
            arrow_to_sql_type(&DataType::Int32, dialect).unwrap(),
            "INTEGER"
        );
        assert_eq!(arrow_to_sql_type(&DataType::Utf8, dialect).unwrap(), "TEXT");
        assert_eq!(
            arrow_to_sql_type(&DataType::Float64, dialect).unwrap(),
            "DOUBLE PRECISION"
        );
    }

    #[test]
    fn test_arrow_to_sql_integer_types() {
        let dialect = SqlDialect::SQLite;
        assert_eq!(
            arrow_to_sql_type(&DataType::Int8, dialect).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Int16, dialect).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Int32, dialect).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Int64, dialect).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::UInt8, dialect).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::UInt16, dialect).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::UInt32, dialect).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::UInt64, dialect).unwrap(),
            "BIGINT"
        );
    }

    #[test]
    fn test_arrow_to_sql_float_types() {
        let pg_dialect = SqlDialect::PostgreSQL;
        let sqlite_dialect = SqlDialect::SQLite;

        assert_eq!(
            arrow_to_sql_type(&DataType::Float32, sqlite_dialect).unwrap(),
            "REAL"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Float32, pg_dialect).unwrap(),
            "REAL"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Float64, sqlite_dialect).unwrap(),
            "DOUBLE"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Float64, pg_dialect).unwrap(),
            "DOUBLE PRECISION"
        );
    }

    #[test]
    fn test_arrow_to_sql_string_types() {
        let dialect = SqlDialect::SQLite;
        assert_eq!(arrow_to_sql_type(&DataType::Utf8, dialect).unwrap(), "TEXT");
        assert_eq!(
            arrow_to_sql_type(&DataType::LargeUtf8, dialect).unwrap(),
            "TEXT"
        );
    }

    #[test]
    fn test_arrow_to_sql_binary_types() {
        let pg_dialect = SqlDialect::PostgreSQL;
        let sqlite_dialect = SqlDialect::SQLite;
        let mysql_dialect = SqlDialect::MySQL;

        assert_eq!(
            arrow_to_sql_type(&DataType::Binary, pg_dialect).unwrap(),
            "BYTEA"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Binary, sqlite_dialect).unwrap(),
            "BLOB"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Binary, mysql_dialect).unwrap(),
            "LONGBLOB"
        );
    }

    #[test]
    fn test_arrow_to_sql_temporal_types() {
        let dialect = SqlDialect::SQLite;
        assert_eq!(
            arrow_to_sql_type(&DataType::Date32, dialect).unwrap(),
            "DATE"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Date64, dialect).unwrap(),
            "DATE"
        );
        assert_eq!(
            arrow_to_sql_type(
                &DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
                dialect
            )
            .unwrap(),
            "TIME"
        );
        assert_eq!(
            arrow_to_sql_type(
                &DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                dialect
            )
            .unwrap(),
            "TIME"
        );
    }

    #[test]
    fn test_arrow_to_sql_timestamp_types() {
        let pg_dialect = SqlDialect::PostgreSQL;
        let mysql_dialect = SqlDialect::MySQL;
        let sqlite_dialect = SqlDialect::SQLite;

        let ts_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);

        assert_eq!(
            arrow_to_sql_type(&ts_type, pg_dialect).unwrap(),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_to_sql_type(&ts_type, mysql_dialect).unwrap(),
            "DATETIME"
        );
        assert_eq!(arrow_to_sql_type(&ts_type, sqlite_dialect).unwrap(), "TEXT");
    }

    #[test]
    fn test_arrow_to_sql_boolean_type() {
        let pg_dialect = SqlDialect::PostgreSQL;
        let sqlite_dialect = SqlDialect::SQLite;
        let mysql_dialect = SqlDialect::MySQL;

        assert_eq!(
            arrow_to_sql_type(&DataType::Boolean, pg_dialect).unwrap(),
            "BOOLEAN"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Boolean, sqlite_dialect).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            arrow_to_sql_type(&DataType::Boolean, mysql_dialect).unwrap(),
            "BOOLEAN"
        );
    }

    #[test]
    fn test_unsupported_type() {
        let dialect = SqlDialect::SQLite;
        // Try with a type that's not fully supported
        let result = arrow_to_sql_type(&DataType::Null, dialect);
        assert!(result.is_ok()); // Null is mapped to TEXT
    }
}
