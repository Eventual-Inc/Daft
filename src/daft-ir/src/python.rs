use daft_logical_plan::{LogicalPlan, LogicalPlanBuilder, PyLogicalPlanBuilder};
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::proto::{ProtoError, ToFromProto};

/// Expose via python to avoid circular dependency if this were a LogicalPlan rust method.
#[pyfunction]
pub fn to_from_proto(plan: PyLogicalPlanBuilder) -> PyResult<PyLogicalPlanBuilder> {
    let plan = plan.builder.build();
    let plan = LogicalPlan::from_proto(plan.to_proto()?)?.arced();
    let builder = LogicalPlanBuilder::from(plan);
    Ok(builder.into())
}

impl From<ProtoError> for PyErr {
    fn from(value: ProtoError) -> Self {
        PyValueError::new_err(format!("{:?}", value))
    }
}

pub fn register_modules(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(to_from_proto, parent)?)?;
    Ok(())
}
