use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef, PyLocalPhysicalPlan};
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_scan::{python::pylib::PyScanTask, ScanTaskRef};
use pyo3::prelude::*;

use crate::translate::translate_single_logical_node;

#[pyclass(module = "daft.daft")]
struct DistributedPhysicalPlan {
    local_physical_plan: LocalPhysicalPlanRef,
    inputs: Vec<ScanTaskRef>,
}

#[pymethods]
impl DistributedPhysicalPlan {
    #[staticmethod]
    pub fn from_logical_plan_builder(builder: &PyLogicalPlanBuilder) -> PyResult<Self> {
        let mut inputs = Vec::new();
        let logical_plan = builder.builder.build();
        let local_physical_plan = translate_single_logical_node(&logical_plan, &mut inputs)?;
        Ok(Self {
            local_physical_plan,
            inputs,
        })
    }

    pub fn get_inputs(&self) -> Vec<PyScanTask> {
        self.inputs
            .iter()
            .map(|input| PyScanTask(input.clone()))
            .collect()
    }

    pub fn get_local_physical_plan(&self) -> PyLocalPhysicalPlan {
        PyLocalPhysicalPlan {
            plan: self.local_physical_plan.clone(),
        }
    }

    pub fn repr(&self) -> String {
        fn build_plan_string(plan: &LocalPhysicalPlan) -> String {
            let node_name = plan.name();
            let children = plan.get_children();

            if children.is_empty() {
                node_name.to_string()
            } else if children.len() == 1 {
                let child_string = build_plan_string(children[0]);
                format!("{} -> {}", child_string, node_name)
            } else {
                // For joins and concat, show both branches
                let child_strings: Vec<String> = children
                    .iter()
                    .map(|child| build_plan_string(child))
                    .collect();
                format!("({}) -> {}", child_strings.join(", "), node_name)
            }
        }

        build_plan_string(&self.local_physical_plan)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<DistributedPhysicalPlan>()?;
    Ok(())
}
