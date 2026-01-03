//! Python bindings for Hudi integration.

use std::sync::Arc;

use common_scan_info::ScanOperatorRef;
use daft_scan::{python::pylib::ScanOperatorHandle, storage_config::StorageConfig};
use pyo3::prelude::*;

use crate::HudiScanOperator;

/// Python-exposed function to create a HudiScanOperator.
///
/// This function is exposed to Python as `daft.daft.hudi_scan()` and returns
/// a `ScanOperatorHandle` that can be used to read Hudi tables.
///
/// # Arguments
/// * `table_uri` - The URI of the Hudi table (local path or cloud storage URI)
/// * `storage_config` - Storage configuration for IO operations
///
/// # Example (Python)
/// ```python
/// from daft.daft import hudi_scan, StorageConfig
/// scan_op = hudi_scan("/path/to/hudi/table", StorageConfig.native())
/// ```
#[pyfunction]
#[pyo3(signature = (table_uri, storage_config))]
pub fn hudi_scan(
    py: Python,
    table_uri: String,
    storage_config: StorageConfig,
) -> PyResult<ScanOperatorHandle> {
    py.detach(|| {
        let multithreaded_io = storage_config.multithreaded_io;
        let runtime = common_runtime::get_io_runtime(multithreaded_io);
        let storage_config = Arc::new(storage_config);

        let operator = runtime.block_on_current_thread(async {
            HudiScanOperator::try_new(&table_uri, storage_config).await
        })?;

        let scan_op_ref = ScanOperatorRef(Arc::new(operator));
        Ok(ScanOperatorHandle::from(scan_op_ref))
    })
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(hudi_scan, parent)?)?;
    Ok(())
}
