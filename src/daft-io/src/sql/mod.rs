mod connection;
mod errors;
mod schema;
mod type_mapping;
mod write;

pub use errors::{SqlError, SqlResult};
pub use write::write_record_batch;

#[cfg(feature = "python")]
mod python {
    use std::io::Cursor;

    use arrow::ipc::reader::StreamReader;
    use pyo3::prelude::*;

    use super::write_record_batch;

    #[pyfunction]
    pub fn write_sql(
        py: Python,
        connection_string: String,
        table_name: String,
        arrow_data: Bound<'_, pyo3::types::PyBytes>,
        mode: String,
    ) -> PyResult<(u64, u64)> {
        let data_bytes = arrow_data.as_bytes();

        if data_bytes.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "No Arrow data provided",
            ));
        }

        let cursor = Cursor::new(data_bytes.to_vec());
        let mut reader = match StreamReader::try_new(cursor, None) {
            Ok(r) => r,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Failed to create Arrow reader: {}",
                    e
                )));
            }
        };

        let record_batch = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Failed to read Arrow batch: {}",
                    e
                )));
            }
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "No RecordBatch in Arrow stream",
                ));
            }
        };

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let result = rt.block_on(async {
            write_record_batch(&connection_string, &table_name, &record_batch, &mode).await
        });

        match result {
            Ok((rows_written, bytes_written)) => Ok((rows_written, bytes_written)),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                e.to_string(),
            )),
        }
    }

    pub fn register(parent: &Bound<'_, PyModule>) -> PyResult<()> {
        parent.add_function(wrap_pyfunction!(write_sql, parent)?)?;
        Ok(())
    }
}

#[cfg(feature = "python")]
pub use python::register;
