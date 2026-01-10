// Use local arrow dependency to match Lance's version

#[cfg(feature = "python")]
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

#[cfg(feature = "python")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "python")]
use common_runtime::get_io_runtime;
use daft_io::IOConfig;
#[cfg(feature = "python")]
use futures::stream::TryStreamExt;
#[cfg(feature = "python")]
use pyo3::{PyErr, PyResult, Python, types::PyAnyMethods};

#[derive(Debug)]
pub struct LanceKVStore {
    pub name: String,
    pub uri: String,
    pub key_column: String,
    pub batch_size: usize,
    pub io_config: Option<IOConfig>,
}

impl LanceKVStore {
    pub fn new(
        name: String,
        uri: String,
        key_column: String,
        batch_size: usize,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            name,
            uri,
            key_column,
            batch_size,
            io_config,
        }
    }

    #[cfg(feature = "python")]
    pub fn get(&self, py: Python, key: &str) -> PyResult<pyo3::Py<pyo3::types::PyAny>> {
        let uri = self.uri.clone();
        let key_col = self.key_column.clone();
        let key_val = key.to_string();
        let storage_options: Option<HashMap<String, String>> = self
            .io_config
            .as_ref()
            .map(|c| io_config_to_storage_options(c, &uri));

        // Run Lance query in IO runtime
        // We spawn a dedicated thread to block on the async task to avoid "Cannot start a runtime from within a runtime"
        // panic if the current thread is already managed by Tokio.
        let result = std::thread::spawn(move || {
            let runtime = get_io_runtime(true);
            runtime.runtime.block_on(async move {
                let dataset = get_dataset_cached(&uri, storage_options.as_ref()).await?;

                let schema = dataset.schema();
                let field = if key_col == "_rowid" {
                    None
                } else {
                    schema.field(&key_col)
                };

                let batches = if key_col == "_rowid" {
                    let row_id = key_val.parse::<u64>().map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!("Invalid _rowid: {}", e))
                    })?;
                    let batch = dataset
                        .take(&[row_id], dataset.schema().clone())
                        .await
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Failed to take row: {}",
                                e
                            ))
                        })?;
                    vec![batch]
                } else {
                    let needs_quoting = if let Some(f) = field {
                        matches!(
                            f.data_type(),
                            arrow::datatypes::DataType::Utf8
                                | arrow::datatypes::DataType::LargeUtf8
                                | arrow::datatypes::DataType::Binary
                                | arrow::datatypes::DataType::LargeBinary
                        )
                    } else {
                        // Default to quoting if field not found (should be caught by schema check above if strictly validated, but Lance 0.20 might be strict)
                        true
                    };

                    let filter = if needs_quoting {
                        format!("{} = '{}'", key_col, key_val)
                    } else {
                        format!("{} = {}", key_col, key_val)
                    };

                    let mut scanner = dataset.scan();
                    scanner.filter(&filter).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!("Invalid filter: {}", e))
                    })?;

                    scanner
                        .try_into_stream()
                        .await
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Failed to scan: {}",
                                e
                            ))
                        })?
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Failed to collect batches: {}",
                                e
                            ))
                        })?
                };

                // Lance (v0.28.0) returns arrow (v54), same as daft-core.
                // No FFI conversion needed.
                Ok::<Vec<RecordBatch>, PyErr>(batches)
            })
        })
        .join()
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Thread panicked: {:?}", e))
        })??;

        if result.is_empty() || result[0].num_rows() == 0 {
            return Ok(py.None().into());
        }

        let batch = &result[0];
        // Convert the first row to a Python dict/object
        // We assume the schema is consistent across batches if there are multiple (unlikely for unique key)

        // Create a dict for the row
        let dict = pyo3::types::PyDict::new(py);
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i);

            // Cast array if needed (e.g. Utf8 -> LargeUtf8) to match Daft's expected types
            let casted_col =
                daft_core::utils::arrow::cast_array_for_daft_if_needed(Box::from(col.as_ref()));

            // Create Daft Series from the Arrow array
            let series = daft_core::series::Series::try_from((field.name().as_str(), casted_col))
                .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create series: {}", e))
            })?;

            // Get the literal value at index 0 and convert to Python object
            use pyo3::IntoPyObject;
            let val = series.get_lit(0).into_pyobject(py)?.unbind();
            dict.set_item(field.name(), val)?;
        }

        Ok(dict.into())
    }
}

#[cfg(feature = "python")]
fn dataset_cache() -> &'static Mutex<HashMap<String, Arc<lance::dataset::Dataset>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<lance::dataset::Dataset>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "python")]
fn dataset_cache_key(uri: &str, storage_options: Option<&HashMap<String, String>>) -> String {
    if let Some(opts) = storage_options {
        let mut pairs: Vec<_> = opts.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let mut key = String::with_capacity(uri.len() + 64);
        key.push_str(uri);
        for (k, v) in pairs {
            key.push('\n');
            key.push_str(k);
            key.push('=');
            key.push_str(v);
        }
        key
    } else {
        uri.to_string()
    }
}

#[cfg(feature = "python")]
async fn get_dataset_cached(
    uri: &str,
    storage_options: Option<&HashMap<String, String>>,
) -> Result<Arc<lance::dataset::Dataset>, PyErr> {
    let cache_key = dataset_cache_key(uri, storage_options);

    if let Some(dataset) = dataset_cache().lock().unwrap().get(&cache_key).cloned() {
        return Ok(dataset);
    }

    let mut builder = lance::dataset::builder::DatasetBuilder::from_uri(uri);
    if let Some(opts) = storage_options {
        builder = builder.with_storage_options(opts.clone());
    }

    let dataset = builder.load().await.map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to open dataset: {}", e))
    })?;
    let dataset = Arc::new(dataset);

    let mut guard = dataset_cache().lock().unwrap();
    let entry = guard.entry(cache_key).or_insert_with(|| dataset.clone());
    Ok(entry.clone())
}

#[cfg(feature = "python")]
fn io_config_to_storage_options(
    io_config: &IOConfig,
    uri: &str,
) -> std::collections::HashMap<String, String> {
    let mut options = std::collections::HashMap::new();

    // Check if URI is S3
    if uri.starts_with("s3://") || uri.starts_with("s3a://") {
        let s3 = &io_config.s3;
        if let Some(region) = &s3.region_name {
            options.insert("aws_region".to_string(), region.clone());
        }
        if let Some(endpoint) = &s3.endpoint_url {
            options.insert("aws_endpoint".to_string(), endpoint.clone());
        }
        if let Some(key_id) = &s3.key_id {
            options.insert("aws_access_key_id".to_string(), key_id.clone());
        }
        if let Some(access_key) = &s3.access_key {
            options.insert("aws_secret_access_key".to_string(), access_key.to_string());
        }
        if let Some(session_token) = &s3.session_token {
            options.insert("aws_session_token".to_string(), session_token.to_string());
        }
        if !s3.use_ssl {
            options.insert("aws_allow_http".to_string(), "true".to_string());
        }
        if s3.force_virtual_addressing {
            options.insert(
                "aws_virtual_hosted_style_request".to_string(),
                "true".to_string(),
            );
        }
    }
    // TODO: Add support for Azure/GCS/HTTP if needed

    options
}
