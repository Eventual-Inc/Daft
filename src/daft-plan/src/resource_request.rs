use daft_core::{impl_bincode_py_state_serialization, utils::hashable_float_wrapper::FloatWrapper};
#[cfg(feature = "python")]
use pyo3::{
    pyclass, pyclass::CompareOp, pymethods, types::PyBytes, PyObject, PyResult, PyTypeInfo, Python,
    ToPyObject,
};
use std::hash::{Hash, Hasher};
use std::ops::Add;

use serde::{Deserialize, Serialize};

/// Resource request for a query fragment task.
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

    pub fn max(resource_requests: &[&Self]) -> Self {
        resource_requests.iter().fold(Default::default(), |acc, e| {
            let max_num_cpus = lift(float_max, acc.num_cpus, e.num_cpus);
            let max_num_gpus = lift(float_max, acc.num_gpus, e.num_gpus);
            let max_memory_bytes = lift(std::cmp::max, acc.memory_bytes, e.memory_bytes);
            Self::new_internal(max_num_cpus, max_num_gpus, max_memory_bytes)
        })
    }
}

impl Add for &ResourceRequest {
    type Output = ResourceRequest;
    fn add(self, other: Self) -> Self::Output {
        Self::Output::new_internal(
            lift(Add::add, self.num_cpus, other.num_cpus),
            lift(Add::add, self.num_gpus, other.num_gpus),
            lift(Add::add, self.memory_bytes, other.memory_bytes),
        )
    }
}

impl Add for ResourceRequest {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        &self + &other
    }
}

impl Eq for ResourceRequest {}

impl Hash for ResourceRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.num_cpus.map(FloatWrapper).hash(state);
        self.num_gpus.map(FloatWrapper).hash(state);
        self.memory_bytes.hash(state)
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
    pub fn new(num_cpus: Option<f64>, num_gpus: Option<f64>, memory_bytes: Option<usize>) -> Self {
        Self::new_internal(num_cpus, num_gpus, memory_bytes)
    }

    /// Take a field-wise max of the list of resource requests.
    #[staticmethod]
    pub fn max_resources(resource_requests: Vec<Self>) -> Self {
        Self::max(&resource_requests.iter().collect::<Vec<_>>())
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
        self + other
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl_bincode_py_state_serialization!(ResourceRequest);
