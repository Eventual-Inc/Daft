use daft_core::python::PySchema;
use daft_plan::{PyLogicalPlanBuilder, PySinkInfo};
use pyo3::{
    types::{PyModule, PyModuleMethods},
    Bound, Python,
};
use {pyo3::pyclass, pyo3::pymethods, pyo3::PyResult};

use crate::{LocalPhysicalPlan, LocalPhysicalPlanRef};

#[pyclass(module = "daft.daft")]
pub struct PyLocalPhysicalPlan {
    pub inner: LocalPhysicalPlanRef,
    pub sink_info: PySinkInfo,
}

impl PyLocalPhysicalPlan {
    pub fn new_internal(local_physical_plan: LocalPhysicalPlanRef) -> Self {
        let sink_info = match local_physical_plan.as_ref() {
            LocalPhysicalPlan::FileWrite(file_write) => PySinkInfo::from_output_file_info(
                file_write.file_info.clone(),
                file_write.file_schema.clone(),
            ),
            _ => PySinkInfo::from_in_memory(),
        };

        Self {
            inner: local_physical_plan,
            sink_info,
        }
    }
}

#[pymethods]
impl PyLocalPhysicalPlan {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
        py: Python,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let logical_plan = logical_plan_builder.builder.build();
            let local_physical_plan = crate::translate(&logical_plan)?;
            Ok(Self::new_internal(local_physical_plan))
        })
    }

    #[getter]
    pub fn get_sink_info(&self) -> PyResult<PySinkInfo> {
        Ok(self.sink_info.clone())
    }

    #[getter]
    pub fn get_schema(&self) -> PyResult<PySchema> {
        Ok(self.inner.schema().clone().into())
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    Ok(())
}
