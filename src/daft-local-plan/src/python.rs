use std::collections::HashMap;

use common_error::DaftError;
use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_micropartition::python::PyMicroPartition;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::{LocalPhysicalPlanRef, translate};
use crate::{ResolvedInput, SourceId, UnresolvedInput};

#[pyclass(module = "daft.daft", name = "LocalPhysicalPlan")]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyLocalPhysicalPlan {
    pub plan: LocalPhysicalPlanRef,
}

#[pymethods]
impl PyLocalPhysicalPlan {
    #[staticmethod]
    fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
    ) -> PyResult<(Self, PyUnresolvedInputs)> {
        let logical_plan = logical_plan_builder.builder.build();
        let (physical_plan, input_specs) = translate(&logical_plan)?;

        Ok((
            Self {
                plan: physical_plan,
            },
            PyUnresolvedInputs { inner: input_specs },
        ))
    }
}

impl_bincode_py_state_serialization!(PyLocalPhysicalPlan);

#[pyclass(module = "daft.daft", name = "UnresolvedInputs", frozen)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyUnresolvedInputs {
    pub inner: HashMap<SourceId, UnresolvedInput>,
}

#[pymethods]
impl PyUnresolvedInputs {
    fn resolve(&self, psets: HashMap<String, Vec<PyMicroPartition>>) -> PyResult<PyResolvedInputs> {
        let mut plan_inputs = HashMap::new();

        for (source_id, unresolved_input) in &self.inner {
            let plan_input = match &unresolved_input {
                UnresolvedInput::ScanTask(tasks) => {
                    Some(ResolvedInput::ScanTasks((**tasks).clone()))
                }
                UnresolvedInput::GlobPaths(paths) => {
                    Some(ResolvedInput::GlobPaths((**paths).clone()))
                }
                UnresolvedInput::InMemory { cache_key } => {
                    let partitions = psets.get(cache_key).ok_or(DaftError::ValueError(format!(
                        "Cache key {} not found in psets",
                        cache_key
                    )))?;
                    Some(ResolvedInput::InMemoryPartitions(
                        partitions
                            .iter()
                            .map(|p| p.inner.clone())
                            .collect::<Vec<_>>(),
                    ))
                }
            };

            if let Some(plan_input) = plan_input {
                plan_inputs.insert(*source_id, plan_input);
            }
        }

        // Always return a PyResolvedInputs, even if empty (for empty scan tasks like limit(0))
        Ok(PyResolvedInputs { inner: plan_inputs })
    }
}

impl_bincode_py_state_serialization!(PyUnresolvedInputs);

#[cfg(feature = "python")]
#[pyclass(module = "daft.daft", name = "ResolvedInputs", frozen)]
#[derive(Debug, Clone)]
pub struct PyResolvedInputs {
    pub inner: HashMap<SourceId, ResolvedInput>,
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyUnresolvedInputs>()?;
    parent.add_class::<PyResolvedInputs>()?;
    Ok(())
}
