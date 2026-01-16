use std::{collections::HashMap, sync::Arc};

use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_micropartition::{partitioning::{MicroPartitionSet, PartitionRef}, python::PyMicroPartition};
use daft_scan::python::pylib::PyScanTask;
use pyo3::{intern, prelude::*};
use serde::{Deserialize, Serialize};

use crate::InputType;
#[cfg(feature = "python")]
use crate::{InputSpec, LocalPhysicalPlanRef, PlanInput, translate};

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
    ) -> PyResult<(Self, PyInputSpecs)> {
        let logical_plan = logical_plan_builder.builder.build();
        let (physical_plan, input_specs) = translate(&logical_plan)?;

        Ok((
            Self {
                plan: physical_plan,
            },
            PyInputSpecs { inner: input_specs },
        ))
    }
}

impl_bincode_py_state_serialization!(PyLocalPhysicalPlan);

#[pyclass(module = "daft.daft", name = "InputSpecs", frozen)]
#[derive(Debug, Clone)]
pub struct PyInputSpecs {
    pub inner: HashMap<String, InputSpec>,
}

#[pymethods]
impl PyInputSpecs {
    /// Convert InputSpecs to Inputs using psets to resolve in_memory inputs.
    /// Rust will call daft.execution.utils.get_in_memory_partitions directly.
    fn resolve_inputs(&self, py: Python, psets: &Bound<PyAny>) -> PyResult<Option<PyInputs>> {
        let mut plan_inputs = HashMap::new();

        for (source_id, input_spec) in &self.inner {
            let plan_input = match &input_spec.input_type {
                InputType::ScanTask(tasks) => Some(PlanInput::ScanTasks((**tasks).clone())),
                InputType::GlobPaths(paths) => Some(PlanInput::GlobPaths((**paths).clone())),
                InputType::InMemory(info) => {
                    // Import get_in_memory_partitions function
                    let utils_module = py.import(intern!(py, "daft.execution.utils"))?;
                    let get_in_memory_partitions =
                        utils_module.getattr(intern!(py, "get_in_memory_partitions"))?;
                    // InMemory case - call daft.execution.utils.get_in_memory_partitions directly
                    let cache_key = &info.cache_key;
                    let py_result = get_in_memory_partitions.call1((psets, cache_key))?;

                    let py_micropartitions = py_result.extract::<Vec<PyMicroPartition>>()?;
                    Some(PlanInput::InMemoryPartitions(
                        py_micropartitions
                            .into_iter()
                            .map(|p| {
                                let partition_ref = p.inner;
                                (partition_ref as PartitionRef)
                            })
                            .collect::<Vec<_>>(),
                    ))
                }
            };

            if let Some(plan_input) = plan_input {
                plan_inputs.insert(source_id.clone(), plan_input);
            }
        }

        if plan_inputs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PyInputs { inner: plan_inputs }))
        }
    }
}

#[cfg(feature = "python")]
#[pyclass(module = "daft.daft", name = "Inputs", frozen)]
#[derive(Debug, Clone)]
pub struct PyInputs {
    pub inner: HashMap<String, PlanInput>,
}

// #[cfg(feature = "python")]
// #[pymethods]
// impl PyInputs {
    /// Construct Inputs from psets, scan_tasks, and glob_paths dictionaries.
    // #[staticmethod]
    // fn from_dicts(
    //     psets: Option<HashMap<String, Vec<PyMicroPartition>>>,
    //     scan_tasks: Option<HashMap<String, Vec<PyScanTask>>>,
    //     glob_paths: Option<HashMap<String, Vec<String>>>,
    // ) -> PyResult<Self> {
    //     use common_scan_info::ScanTaskLikeRef;

    //     let mut plan_inputs = HashMap::new();

    //     // Add in-memory partitions from psets
    //     if let Some(psets) = psets {
    //         for (source_id, micropartitions) in psets {
    //             let micropartition_refs: Vec<_> =
    //                 micropartitions.into_iter().map(|p| p.into()).collect();
    //             let partition_set = Arc::new(MicroPartitionSet::from(micropartition_refs));
    //             plan_inputs.insert(source_id, PlanInput::InMemoryPartitions(partition_set));
    //         }
    //     }

    //     // Add scan tasks
    //     if let Some(scan_tasks) = scan_tasks {
    //         for (source_id, tasks) in scan_tasks {
    //             let task_refs: Vec<ScanTaskLikeRef> =
    //                 tasks.into_iter().map(|task| task.0).collect();
    //             plan_inputs.insert(source_id, PlanInput::ScanTasks(task_refs));
    //         }
    //     }

    //     // Add glob paths
    //     if let Some(glob_paths) = glob_paths {
    //         for (source_id, paths) in glob_paths {
    //             plan_inputs.insert(source_id, PlanInput::GlobPaths(paths));
    //         }
    //     }

    //     Ok(Self { inner: plan_inputs })
    // }
//}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyInputSpecs>()?;
    parent.add_class::<PyInputs>()?;
    Ok(())
}
