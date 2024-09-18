mod adaptive;
mod scheduler;

pub use adaptive::AdaptivePhysicalPlanScheduler;
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use scheduler::PhysicalPlanScheduler;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PhysicalPlanScheduler>()?;
    parent.add_class::<AdaptivePhysicalPlanScheduler>()?;

    Ok(())
}
