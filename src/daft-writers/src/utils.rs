use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_io::{SourceType, utils::ObjectPath};
use daft_recordbatch::RecordBatch;
#[cfg(feature = "python")]
use pyo3::{Python, types::PyAnyMethods};

#[cfg(feature = "python")]
pub type FilenameProvider = Arc<pyo3::Py<pyo3::PyAny>>;
#[cfg(not(feature = "python"))]
pub type FilenameProvider = ();

/// The default value used by Hive for null partition values.
const DEFAULT_PARTITION_VALUE: &str = "__HIVE_DEFAULT_PARTITION__";

/// Helper function to build the filename for the output file.
pub(crate) fn build_filename(
    source_type: SourceType,
    root_dir: &str,
    partition_values: Option<&RecordBatch>,
    file_idx: usize,
    suffix: &str,
) -> DaftResult<PathBuf> {
    let partition_path = get_partition_path(partition_values)?;
    let filename = generate_filename(file_idx, suffix);

    match source_type {
        SourceType::File => build_local_file_path(root_dir, partition_path, filename),
        source if source.supports_native_writer() => {
            build_object_path(root_dir, partition_path, filename)
        }
        _ => Err(DaftError::ValueError(format!(
            "Unsupported source type: {:?}",
            source_type
        ))),
    }
}

/// Helper function to build a filename using a Python-side `FilenameProvider` if
/// one is provided, otherwise falling back to [`build_filename`].
#[cfg(feature = "python")]
pub(crate) fn build_filename_with_provider(
    source_type: SourceType,
    root_dir: &str,
    partition_values: Option<&RecordBatch>,
    file_idx: usize,
    ext: &str,
    filename_provider: Option<Arc<pyo3::Py<pyo3::PyAny>>>,
    write_uuid: Option<&str>,
) -> DaftResult<PathBuf> {
    if let Some(provider) = filename_provider {
        let write_uuid = write_uuid.ok_or_else(|| {
            DaftError::InternalError(
                "Filename provider was supplied without a write_uuid".to_string(),
            )
        })?;

        let filename: String = Python::attach(|py| -> pyo3::PyResult<_> {
            let provider = provider.clone();
            let provider = provider.bind(py);

            // For native writers we currently expose only index-based metadata to the
            // FilenameProvider. `task_index` and `block_index` are set to `0` and
            // `file_idx` respectively.
            let task_index: i64 = (file_idx as i64) >> 32;
            let block_index: i64 = (file_idx as i64) & 0xFFFFFFFF;
            let file_idx_i64: i64 = file_idx as i64;

            provider
                .call_method(
                    pyo3::intern!(py, "get_filename_for_block"),
                    (write_uuid, task_index, block_index, file_idx_i64, ext),
                    None,
                )?
                .extract()
        })?;

        let partition_path = get_partition_path(partition_values)?;
        match source_type {
            SourceType::File => build_local_file_path(root_dir, partition_path, filename),
            source if source.supports_native_writer() => {
                build_object_path(root_dir, partition_path, filename)
            }
            _ => Err(DaftError::ValueError(format!(
                "Unsupported source type: {:?}",
                source_type
            ))),
        }
    } else {
        build_filename(source_type, root_dir, partition_values, file_idx, ext)
    }
}

/// Helper function to get the partition path from the record batch.
fn get_partition_path(partition_values: Option<&RecordBatch>) -> DaftResult<PathBuf> {
    match partition_values {
        Some(partition_values) => Ok(record_batch_to_partition_path(partition_values, None)?),
        None => Ok(PathBuf::new()),
    }
}

/// Converts a single-row RecordBatch to a Hive-style partition path.
///
/// This method creates a PathBuf representation of partition values in the format
/// `/key1=value1/key2=value2...` where keys are URL-encoded column names and
/// values are URL-encoded partition values.
///
/// # Arguments
///
/// * `partition_null_fallback` - Optional value to use for null partition values.
///   If not provided, uses the Hive default `__HIVE_DEFAULT_PARTITION__`.
///
/// # Returns
///
/// * `Ok(PathBuf)` - The partition path if successful
/// * `Err(DaftError)` - If the RecordBatch has more than one row, or if we fail to downcast the partition values to UTF-8 strings.
fn record_batch_to_partition_path(
    record_batch: &RecordBatch,
    partition_null_fallback: Option<&str>,
) -> DaftResult<PathBuf> {
    if record_batch.len() != 1 {
        return Err(DaftError::InternalError(
            "Only single row RecordBatches can be converted to partition strings".to_string(),
        ));
    }
    let default_partition = if let Some(partition_null_fallback) = partition_null_fallback {
        urlencoding::encode(partition_null_fallback)
    } else {
        DEFAULT_PARTITION_VALUE.into()
    };
    let partition_path = record_batch
        .columns()
        .iter()
        .map(|col| {
            let key = urlencoding::encode(col.name());
            if col.inner.validity().is_none_or(|v| v.is_valid(0)) {
                let value = col.inner.str_value(0)?;
                Ok(format!("{}={}", key, urlencoding::encode(&value)))
            } else {
                Ok(format!("{}={}", key, default_partition))
            }
        })
        .collect::<DaftResult<PathBuf>>()?;
    Ok(partition_path)
}

// Helper function to generate a filename.
fn generate_filename(file_idx: usize, suffix: &str) -> String {
    format!("{}-{}.{}", uuid::Uuid::new_v4(), file_idx, suffix)
}

/// Helper function to build the path to a local file.
fn build_local_file_path(
    root_dir: &str,
    partition_path: PathBuf,
    filename: String,
) -> DaftResult<PathBuf> {
    let root_dir = Path::new(root_dir.trim_start_matches("file://"));
    let dir = root_dir.join(partition_path);
    Ok(dir.join(filename))
}

/// Helper function to build the path to an object url. The pattern of object url is `{bucket}/{key}`. TODO consider include the scheme in the return value
fn build_object_path(
    root_dir: &str,
    partition_path: PathBuf,
    filename: String,
) -> DaftResult<PathBuf> {
    let ObjectPath {
        scheme: _scheme,
        bucket,
        key,
    } = daft_io::utils::parse_object_url(root_dir)?;
    let key = Path::new(&key).join(partition_path).join(filename);
    Ok(PathBuf::from(format!("{}/{}", bucket, key.display())))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::{DaftError, DaftResult};
    use daft_core::{
        prelude::{DataType, Field},
        series::Series,
    };
    use daft_recordbatch::RecordBatch;

    use crate::utils::record_batch_to_partition_path;

    #[test]
    fn test_record_batch_to_partition_string() -> DaftResult<()> {
        let year_series = Series::from_arrow2(
            Arc::new(Field::new("year", DataType::Utf8)),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(&["2023"])),
        )?;
        let month_series = Series::from_arrow2(
            Arc::new(Field::new("month", DataType::Utf8)),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(&["1"])),
        )?;
        // Include a column with a null value.
        let day_series = Series::from_arrow2(
            Arc::new(Field::new("day", DataType::Utf8)),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from([None::<&str>])),
        )?;
        // Include a column with a name that needs to be URL-encoded.
        let date_series = Series::from_arrow2(
            Arc::new(Field::new("today's date", DataType::Utf8)),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(&[
                "2025/04/29",
            ])),
        )?;
        let batch = RecordBatch::from_nonempty_columns(vec![
            year_series,
            month_series,
            day_series,
            date_series,
        ])?;
        let partition_path = record_batch_to_partition_path(&batch, None)?;
        assert_eq!(
            partition_path,
            std::path::PathBuf::from("year=2023")
                .join("month=1")
                .join("day=__HIVE_DEFAULT_PARTITION__")
                .join("today%27s%20date=2025%2F04%2F29")
        );
        // Test with a fallback value that includes spaces.
        let partition_path =
            record_batch_to_partition_path(&batch, Some("unconventional fallback"))?;
        assert_eq!(
            partition_path,
            std::path::PathBuf::from("year=2023")
                .join("month=1")
                .join("day=unconventional%20fallback")
                .join("today%27s%20date=2025%2F04%2F29")
        );
        Ok(())
    }

    #[test]
    fn test_record_batch_to_partition_string_multi_row_error() -> DaftResult<()> {
        let year_series = Series::from_arrow2(
            Arc::new(Field::new("year", DataType::Utf8)),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(&[
                "2023", "2024",
            ])),
        )?;
        let month_series = Series::from_arrow2(
            Arc::new(Field::new("month", DataType::Utf8)),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(&["1", "2"])),
        )?;
        let batch = RecordBatch::from_nonempty_columns(vec![year_series, month_series])?;

        let result = record_batch_to_partition_path(&batch, None);
        assert!(result.is_err());
        assert!(matches!(
            result.as_ref().unwrap_err(),
            DaftError::InternalError(_)
        ));
        assert_eq!(
            result.as_ref().unwrap_err().to_string(),
            "DaftError::InternalError Only single row RecordBatches can be converted to partition strings"
        );
        Ok(())
    }
}
