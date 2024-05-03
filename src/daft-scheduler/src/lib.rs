mod adaptive;
mod scheduler;

pub use adaptive::AdaptivePhysicalPlanScheduler;
pub use scheduler::PhysicalPlanScheduler;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PhysicalPlanScheduler>()?;
    parent.add_class::<AdaptivePhysicalPlanScheduler>()?;

    Ok(())
}
