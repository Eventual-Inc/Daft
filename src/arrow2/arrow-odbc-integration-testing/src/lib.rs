#![cfg(test)]

mod read;
mod write;

use arrow2::io::odbc::api::{Connection, Environment, Error as OdbcError};
use lazy_static::lazy_static;

lazy_static! {
    /// This is an example for using doc comment attributes
    pub static ref ENV: Environment = Environment::new().unwrap();
}

/// Connection string for our test instance of Microsoft SQL Server
const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;";

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table(
    conn: &Connection<'_>,
    table_name: &str,
    column_types: &[&str],
) -> std::result::Result<(), OdbcError> {
    let drop_table = &format!("DROP TABLE IF EXISTS {}", table_name);

    let column_names = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"];
    let cols = column_types
        .iter()
        .zip(column_names)
        .map(|(ty, name)| format!("{} {}", name, ty))
        .collect::<Vec<_>>()
        .join(", ");

    let create_table = format!(
        "CREATE TABLE {} (id int IDENTITY(1,1),{});",
        table_name, cols
    );
    conn.execute(drop_table, ())?;
    conn.execute(&create_table, ())?;
    Ok(())
}
