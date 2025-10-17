use std::sync::Arc;

/// Provider implementation reference.
pub type ProviderRef = Arc<dyn Provider>;

/// Provider trait for interacting with providers in rust, which is currently only used in the session.
pub trait Provider: Sync + Send + std::fmt::Debug {
    /// Returns the provider name.
    fn name(&self) -> String;

    /// Creates (or extracts) a Python object that subclasses the Provider ABC.
    #[cfg(feature = "python")]
    fn to_py(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}
