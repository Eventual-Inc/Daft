use sqlx::AnyPool;

use crate::sql::errors::{SqlError, SqlResult};

pub struct SqlConnectionPool {
    pool: AnyPool,
}

impl SqlConnectionPool {
    pub async fn new(connection_string: &str) -> SqlResult<Self> {
        let pool = AnyPool::connect(connection_string)
            .await
            .map_err(|e| SqlError::Connection(e.to_string()))?;

        Ok(SqlConnectionPool { pool })
    }

    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }
}
