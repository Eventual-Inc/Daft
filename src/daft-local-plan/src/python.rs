use std::{collections::HashMap, sync::Arc};

use common_py_serde::impl_bincode_py_state_serialization;
use daft_logical_plan::PyLogicalPlanBuilder;
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use daft_recordbatch::python::PyRecordBatch;
use pyo3::{prelude::*, types::PyDict};
use serde::{Deserialize, Serialize};

use crate::Input;
#[cfg(feature = "python")]
use crate::{ExecutionEngineFinalResult, LocalPhysicalPlanRef, translate};

#[pyclass(module = "daft.daft", name = "LocalPhysicalPlan")]
#[derive(Debug, Serialize, Deserialize)]
pub struct PyLocalPhysicalPlan {
    pub plan: LocalPhysicalPlanRef,
}

#[pymethods]
impl PyLocalPhysicalPlan {
    fn fingerprint(&self) -> u64 {
        self.plan.fingerprint()
    }

    #[staticmethod]
    fn from_logical_plan_builder(
        py: Python<'_>,
        logical_plan_builder: &PyLogicalPlanBuilder,
        psets: HashMap<String, Vec<PyMicroPartition>>,
    ) -> PyResult<(Self, Py<PyDict>)> {
        let psets_mp: HashMap<String, Vec<MicroPartitionRef>> = psets
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|p| p.inner).collect()))
            .collect();
        let logical_plan = logical_plan_builder.builder.build();
        let (physical_plan, inputs) = translate(&logical_plan, &psets_mp)?;

        let dict = PyDict::new(py);
        for (source_id, input) in inputs {
            match &input {
                Input::InMemory(partitions) => {
                    let py_parts: Vec<PyMicroPartition> = partitions
                        .iter()
                        .map(|p| PyMicroPartition::from(p.clone()))
                        .collect();
                    dict.set_item(source_id, py_parts)?;
                }
                _ => {
                    let py_input = PyInput { inner: input };
                    dict.set_item(source_id, py_input.into_pyobject(py)?)?;
                }
            }
        }

        Ok((
            Self {
                plan: physical_plan,
            },
            dict.into(),
        ))
    }

    fn single_line_display(&self) -> String {
        self.plan.single_line_display()
    }
}

impl_bincode_py_state_serialization!(PyLocalPhysicalPlan);

#[pyclass(module = "daft.daft", name = "Input", frozen)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyInput {
    pub inner: Input,
}

impl_bincode_py_state_serialization!(PyInput);

impl<'py> FromPyObject<'_, 'py> for Input {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(py_input) = ob.extract::<PyInput>() {
            Ok(py_input.inner)
        } else if let Ok(partitions) = ob.extract::<Vec<PyMicroPartition>>() {
            Ok(Self::InMemory(
                partitions.into_iter().map(|p| p.inner).collect(),
            ))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Expected Input or list[MicroPartition]",
            ))
        }
    }
}

#[pyclass(module = "daft.daft", name = "PyExecutionEngineFinalResult", frozen)]
pub struct PyExecutionEngineFinalResult {
    inner: Arc<ExecutionEngineFinalResult>,
}

#[pymethods]
impl PyExecutionEngineFinalResult {
    fn encode(&self) -> Vec<u8> {
        self.inner.encode()
    }

    fn to_recordbatch(&self) -> PyResult<PyRecordBatch> {
        Ok(self.inner.to_recordbatch()?.into())
    }
}

impl From<ExecutionEngineFinalResult> for PyExecutionEngineFinalResult {
    fn from(inner: ExecutionEngineFinalResult) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyLocalPhysicalPlan>()?;
    parent.add_class::<PyInput>()?;
    parent.add_class::<PyExecutionEngineFinalResult>()?;
    Ok(())
}
