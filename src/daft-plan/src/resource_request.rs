use daft_core::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use {
    pyo3::{pyclass, pyclass::CompareOp, pymethods, types::PyBytes, PyResult, Python},
    std::{cmp::max, ops::Add},
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct ResourceRequest {
    pub num_cpus: Option<f64>,
    pub num_gpus: Option<f64>,
    pub memory_bytes: Option<usize>,
}

impl ResourceRequest {
    pub fn new_internal(
        num_cpus: Option<f64>,
        num_gpus: Option<f64>,
        memory_bytes: Option<usize>,
    ) -> Self {
        Self {
            num_cpus,
            num_gpus,
            memory_bytes,
        }
    }
}

fn lift<T, F: FnOnce(T, T) -> T>(f: F, left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (None, right) => right,
        (left, None) => left,
        (Some(left), Some(right)) => Some(f(left, right)),
    }
}

fn float_max(left: f64, right: f64) -> f64 {
    left.max(right)
}

#[cfg(feature = "python")]
#[pymethods]
impl ResourceRequest {
    #[new]
    #[pyo3(signature = (num_cpus=None, num_gpus=None, memory_bytes=None))]
    pub fn new(num_cpus: Option<f64>, num_gpus: Option<f64>, memory_bytes: Option<usize>) -> Self {
        Self::new_internal(num_cpus, num_gpus, memory_bytes)
    }

    #[staticmethod]
    pub fn max_resources(resource_requests: Vec<Self>) -> Self {
        resource_requests.iter().fold(Default::default(), |acc, e| {
            let max_num_cpus = lift(float_max, acc.num_cpus, e.num_cpus);
            let max_num_gpus = lift(float_max, acc.num_gpus, e.num_gpus);
            let max_memory_bytes = lift(max, acc.memory_bytes, e.memory_bytes);
            Self::new_internal(max_num_cpus, max_num_gpus, max_memory_bytes)
        })
    }

    #[getter]
    pub fn get_num_cpus(&self) -> PyResult<Option<f64>> {
        Ok(self.num_cpus)
    }

    #[getter]
    pub fn get_num_gpus(&self) -> PyResult<Option<f64>> {
        Ok(self.num_gpus)
    }

    #[getter]
    pub fn get_memory_bytes(&self) -> PyResult<Option<usize>> {
        Ok(self.memory_bytes)
    }

    fn __add__(&self, other: &Self) -> Self {
        Self::new_internal(
            lift(Add::add, self.num_cpus, other.num_cpus),
            lift(Add::add, self.num_gpus, other.num_gpus),
            lift(Add::add, self.memory_bytes, other.memory_bytes),
        )
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }
}

impl_bincode_py_state_serialization!(ResourceRequest);
