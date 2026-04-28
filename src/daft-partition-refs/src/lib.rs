mod flight;
#[cfg(feature = "python")]
mod ray;

pub use flight::FlightPartitionRef;
#[cfg(feature = "python")]
pub use flight::PyFlightPartitionRef;
#[cfg(feature = "python")]
pub use ray::RayPartitionRef;

#[cfg(feature = "python")]
pub fn register_modules(parent: &pyo3::Bound<pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    flight::register_modules(parent)?;
    ray::register_modules(parent)?;
    Ok(())
}
