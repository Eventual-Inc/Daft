use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{InputSpec, LocalPhysicalPlanRef, translate};

#[pyclass(module = "daft.daft", name = "LocalPhysicalPlan")]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyLocalPhysicalPlan {
    pub plan: LocalPhysicalPlanRef,
}

#[pymethods]
impl PyLocalPhysicalPlan {
    #[staticmethod]
    fn from_logical_plan_builder(logical_plan_builder: &PyLogicalPlanBuilder) -> PyResult<(Self, Vec<PyInputSpec>)> {
        let logical_plan = logical_plan_builder.builder.build();
        let (physical_plan, input_specs) = translate(&logical_plan)?;
        
        // Convert HashMap of InputSpecs to a Vec of PyInputSpecs
        let py_input_specs: Vec<PyInputSpec> = input_specs
            .values()
            .map(|spec| PyInputSpec::from(spec))
            .collect();
        
        Ok((
            Self {
                plan: physical_plan,
            },
            py_input_specs,
        ))
    }

    /// Get the source_id from a PhysicalScan plan, or None if not a PhysicalScan
    fn get_source_id(&self) -> Option<String> {
        match self.plan.as_ref() {
            crate::LocalPhysicalPlan::PhysicalScan(scan) => Some(scan.source_id.clone()),
            _ => None,
        }
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.plan.hash(&mut hasher);
        hasher.finish()
    }

    fn name(&self) -> String {
        self.plan.single_line_display()
    }

    /// Get a structural fingerprint for this plan.
    /// Plans from the same pipeline nodes will have the same fingerprint,
    /// even if their PyObject identities differ.
    fn fingerprint(&self) -> u64 {
        self.plan.fingerprint()
    }
}

impl_bincode_py_state_serialization!(PyLocalPhysicalPlan);

#[pyclass(module = "daft.daft", name = "InputSpec", frozen)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyInputSpec {
    #[pyo3(get)]
    pub source_id: String,
    #[pyo3(get)]
    pub input_type: String, // "scan_task", "in_memory", or "glob_paths"
    // Internal field to store the actual input spec
    pub(crate) spec: InputSpec,
}

#[pymethods]
impl PyInputSpec {
    /// Get scan tasks if this is a scan_task input, otherwise None
    #[cfg(feature = "python")]
    fn get_scan_tasks(&self) -> Option<Vec<daft_scan::python::pylib::PyScanTask>> {
        match &self.spec.input_type {
            crate::InputType::ScanTask(tasks) => {
                Some(tasks.iter().map(|t| daft_scan::python::pylib::PyScanTask(t.clone())).collect())
            }
            _ => None,
        }
    }
    
    /// Get the cache key if this is an in_memory input, otherwise None
    fn get_cache_key(&self) -> Option<String> {
        match &self.spec.input_type {
            crate::InputType::InMemory(info) => Some(info.cache_key.clone()),
            _ => None,
        }
    }
    
    /// Get glob paths if this is a glob_paths input, otherwise None
    fn get_glob_paths(&self) -> Option<Vec<String>> {
        match &self.spec.input_type {
            crate::InputType::GlobPaths(paths) => Some((**paths).clone()),
            _ => None,
        }
    }
}

impl From<&InputSpec> for PyInputSpec {
    fn from(spec: &InputSpec) -> Self {
        let input_type = match &spec.input_type {
            crate::InputType::ScanTask(_) => "scan_task".to_string(),
            crate::InputType::InMemory(_) => "in_memory".to_string(),
            crate::InputType::GlobPaths(_) => "glob_paths".to_string(),
        };
        Self {
            source_id: spec.source_id.clone(),
            input_type,
            spec: spec.clone(),
        }
    }
}

impl_bincode_py_state_serialization!(PyInputSpec);

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyInputSpec>()?;
    Ok(())
}
