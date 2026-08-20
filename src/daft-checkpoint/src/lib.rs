pub mod builder;
#[cfg(feature = "python")]
mod config;
pub mod error;
pub mod impls;
#[cfg(feature = "s3")]
pub mod scan;
mod store;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
mod types;

pub use builder::build_store;
pub use error::*;
#[cfg(feature = "s3")]
pub use scan::BlobStoreCheckpointedKeysScanOperator;
pub use store::*;
pub use types::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &pyo3::Bound<pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    use pyo3::types::PyModuleMethods;
    common_checkpoint_config::python::register_modules(parent)?;
    parent.add_class::<config::PyCheckpointStore>()?;
    parent.add_class::<config::PyCheckpoint>()?;
    parent.add_class::<config::PyCheckpointStatus>()?;
    parent.add_class::<config::PyCheckpointFileFormat>()?;
    parent.add_class::<config::PyFileMetadata>()?;
    parent.add_function(pyo3::wrap_pyfunction!(
        config::build_checkpoint_store,
        parent
    )?)?;
    Ok(())
}
