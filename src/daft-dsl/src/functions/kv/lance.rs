// Use local arrow dependency to match Lance's version
use std::sync::Arc;

#[cfg(feature = "python")]
use arrow::{array::Array, datatypes::DataType, record_batch::RecordBatch};
#[cfg(feature = "python")]
use arrow_53::array::Array as Array53;
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
        let storage_options = self
            .io_config
            .as_ref()
            .map(|c| io_config_to_storage_options(c, &uri));

        // Run Lance query in IO runtime
        // We spawn a dedicated thread to block on the async task to avoid "Cannot start a runtime from within a runtime"
        // panic if the current thread is already managed by Tokio.
        let result = std::thread::spawn(move || {
            let runtime = get_io_runtime(true);
            runtime.runtime.block_on(async move {
                let mut builder = lance::dataset::builder::DatasetBuilder::from_uri(&uri);
                if let Some(opts) = storage_options {
                    builder = builder.with_storage_options(opts);
                }

                let dataset = builder.load().await.map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to open dataset: {}",
                        e
                    ))
                })?;

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
                            arrow_53::datatypes::DataType::Utf8
                                | arrow_53::datatypes::DataType::LargeUtf8
                                | arrow_53::datatypes::DataType::Binary
                                | arrow_53::datatypes::DataType::LargeBinary
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

                // Lance (v0.20.0) returns arrow (v53), but we need arrow (v54) for daft-core.
                // We use FFI to convert between them.
                convert_arrow_53_to_54(batches)
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
            let val = arrow_scalar_to_py(py, col, 0)?;
            dict.set_item(field.name(), val)?;
        }

        Ok(dict.into())
    }
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

#[cfg(feature = "python")]
fn convert_arrow_53_to_54(
    batches: Vec<arrow_53::record_batch::RecordBatch>,
) -> PyResult<Vec<arrow::record_batch::RecordBatch>> {
    use arrow::ffi;

    let mut converted_batches = Vec::with_capacity(batches.len());
    for batch in batches {
        // batch is inferred as RecordBatch53
        let struct_array_53 = arrow_53::array::StructArray::from(batch);
        let array_data_53 = struct_array_53.into_data();

        // Export to FFI (v53)
        let ffi_array_53 = arrow_53::ffi::FFI_ArrowArray::new(&array_data_53);
        let ffi_schema_53 = arrow_53::ffi::FFI_ArrowSchema::try_from(array_data_53.data_type())
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to create FFI schema: {}",
                    e
                ))
            })?;

        // Transmute to FFI (v54) - Safe because FFI structs are #[repr(C)]
        let ffi_array_54: ffi::FFI_ArrowArray = unsafe { std::mem::transmute(ffi_array_53) };
        let ffi_schema_54: ffi::FFI_ArrowSchema = unsafe { std::mem::transmute(ffi_schema_53) };

        // Import from FFI using Arrow 54
        // arrow::ffi::from_ffi returns Result<ArrayData>
        let array_data_54 = unsafe {
            ffi::from_ffi(ffi_array_54, &ffi_schema_54).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to import FFI array: {}",
                    e
                ))
            })?
        };

        let struct_array_54 = arrow::array::StructArray::from(array_data_54);
        let batch_54 = RecordBatch::from(&struct_array_54);
        converted_batches.push(batch_54);
    }
    Ok(converted_batches)
}

// Helper to convert Scalar value from Arrow Array to Python Object
#[cfg(feature = "python")]
fn arrow_scalar_to_py(
    py: Python,
    col: &Arc<dyn Array>,
    row_idx: usize,
) -> PyResult<pyo3::Py<pyo3::types::PyAny>> {
    use daft_core::{datatypes::Field, series::Series};
    use pyo3::IntoPyObject;

    if col.is_null(row_idx) {
        return Ok(py.None());
    }

    let (col, daft_type) = match daft_core::datatypes::DataType::try_from(col.data_type()) {
        Ok(dt) => (col.clone(), dt),
        Err(_) => {
            match col.data_type() {
                DataType::Utf8 => {
                    let casted = arrow::compute::cast(col, &DataType::LargeUtf8).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Failed to cast Utf8 to LargeUtf8: {}",
                            e
                        ))
                    })?;
                    (casted, daft_core::datatypes::DataType::Utf8)
                }
                DataType::Binary => {
                    let casted =
                        arrow::compute::cast(col, &DataType::LargeBinary).map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "Failed to cast Binary to LargeBinary: {}",
                                e
                            ))
                        })?;
                    (casted, daft_core::datatypes::DataType::Binary)
                }
                DataType::List(field) => {
                    // Daft expects LargeList for List DataType, but Arrow might provide List (32-bit)
                    // We cast to LargeList
                    let large_list_type = DataType::LargeList(field.clone());
                    let casted = arrow::compute::cast(col, &large_list_type).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Failed to cast List to LargeList: {}",
                            e
                        ))
                    })?;
                    let dt = daft_core::datatypes::DataType::try_from(casted.data_type()).map_err(
                        |e| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "Failed to convert casted Arrow type to Daft type: {}",
                                e
                            ))
                        },
                    )?;
                    (casted, dt)
                }
                DataType::Float16 => {
                    // Daft might not support Float16 yet, cast to Float32
                    let casted = arrow::compute::cast(col, &DataType::Float32).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Failed to cast Float16 to Float32: {}",
                            e
                        ))
                    })?;
                    (casted, daft_core::datatypes::DataType::Float32)
                }
                dt => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Unsupported Arrow type for KV conversion: {:?}",
                        dt
                    )));
                }
            }
        }
    };

    let field = Arc::new(Field::new("col", daft_type));
    let series = Series::from_arrow(field, col).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to convert Arrow array to Daft Series: {}",
            e
        ))
    })?;

    let lit = series.get_lit(row_idx);
    let obj = lit.into_pyobject(py)?;
    Ok(obj.unbind())
}
