/// Defines FileFormat enum, which represents the format of a file, e.g. Parquet, CSV, JSON.
///
/// NOTE: This is currently abused to also represent data being read from a Database or from a Python
/// location. We should refactor our code to remove this.
use std::str::FromStr;

use common_error::{DaftError, DaftResult};
use common_py_serde::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

/// Format of a file, e.g. Parquet, CSV, JSON.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Copy)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum FileFormat {
    Parquet,
    Csv,
    Json,
    Warc,
    Database,
    Python,
}

#[cfg(feature = "python")]
#[pymethods]
impl FileFormat {
    fn ext(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Warc => "warc",
            Self::Database => "db",
            Self::Python => "py",
        }
    }
}

impl FromStr for FileFormat {
    type Err = DaftError;

    fn from_str(file_format: &str) -> DaftResult<Self> {
        use FileFormat::{Csv, Database, Json, Parquet, Warc};

        if file_format.trim().eq_ignore_ascii_case("parquet") {
            Ok(Parquet)
        } else if file_format.trim().eq_ignore_ascii_case("csv") {
            Ok(Csv)
        } else if file_format.trim().eq_ignore_ascii_case("json") {
            Ok(Json)
        } else if file_format.trim().eq_ignore_ascii_case("warc") {
            Ok(Warc)
        } else if file_format.trim().eq_ignore_ascii_case("database") {
            Ok(Database)
        } else {
            Err(DaftError::TypeError(format!(
                "FileFormat {file_format} not supported!"
            )))
        }
    }
}

impl_bincode_py_state_serialization!(FileFormat);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Copy)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum WriteMode {
    Overwrite,
    OverwritePartitions,
    Append,
}

impl FromStr for WriteMode {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "overwrite" => Ok(Self::Overwrite),
            "overwrite-partitions" => Ok(Self::OverwritePartitions),
            "append" => Ok(Self::Append),
            _ => Err(DaftError::TypeError(format!(
                "WriteMode {s} not supported!"
            ))),
        }
    }
}

impl_bincode_py_state_serialization!(WriteMode);
