use common_error::DaftError;
use snafu::Snafu;

pub mod options;
pub mod read;
pub mod schema;
pub mod write;

#[cfg(feature = "python")]
pub mod python;

pub use options::{AvroCompression, AvroSourceConfig, AvroWriteOptions};
pub use read::read_avro;
pub use schema::read_avro_schema;
pub use write::write_record_batch_to_avro;

#[derive(Debug, Snafu)]
pub enum AvroError {
    #[snafu(display("Failed to read Avro file: {}", message))]
    ReadError { message: String },

    #[snafu(display("Failed to write Avro file: {}", message))]
    WriteError { message: String },

    #[snafu(display("Schema conversion error: {}", message))]
    SchemaConversionError { message: String },

    #[snafu(display("Unsupported Avro type: {}", type_name))]
    UnsupportedType { type_name: String },

    #[snafu(display("Unsupported compression codec: {}", codec))]
    UnsupportedCodec { codec: String },

    #[snafu(display("IO error: {}", source))]
    IOError { source: daft_io::Error },

    #[snafu(display("Arrow error: {}", source))]
    ArrowError { source: arrow_schema::ArrowError },
}

impl From<arrow_schema::ArrowError> for AvroError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        Self::ArrowError { source: err }
    }
}

impl From<AvroError> for DaftError {
    fn from(err: AvroError) -> Self {
        match err {
            AvroError::IOError { source } => source.into(),
            _ => Self::External(err.into()),
        }
    }
}

impl From<daft_io::Error> for AvroError {
    fn from(err: daft_io::Error) -> Self {
        Self::IOError { source: err }
    }
}

impl From<DaftError> for AvroError {
    fn from(err: DaftError) -> Self {
        Self::SchemaConversionError {
            message: format!("{}", err),
        }
    }
}

type Result<T, E = AvroError> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub fn register_modules(
    parent: &pyo3::prelude::Bound<pyo3::prelude::PyModule>,
) -> pyo3::prelude::PyResult<()> {
    use pyo3::prelude::*;
    parent.add_class::<AvroSourceConfig>()?;
    parent.add_function(wrap_pyfunction!(python::pylib::read_avro, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::pylib::read_avro_schema, parent)?)?;
    Ok(())
}
