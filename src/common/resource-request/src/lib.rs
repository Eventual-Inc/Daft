use std::{
    hash::{Hash, Hasher},
    ops::Add,
};

use common_hashable_float_wrapper::FloatWrapper;
use common_py_serde::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use pyo3::{
    pyclass,
    pyclass::CompareOp,
    pymethods,
    types::{PyModule, PyModuleMethods},
    Bound, PyObject, PyResult, Python,
};
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
    #[must_use]
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

    #[must_use]
    pub fn default_cpu() -> Self {
        Self::new_internal(Some(1.0), None, None)
    }

    #[must_use]
    pub fn or_num_cpus(&self, num_cpus: Option<f64>) -> Self {
        Self {
            num_cpus: self.num_cpus.or(num_cpus),
            ..self.clone()
        }
    }

    #[must_use]
    pub fn or_num_gpus(&self, num_gpus: Option<f64>) -> Self {
        Self {
            num_gpus: self.num_gpus.or(num_gpus),
            ..self.clone()
        }
    }

    #[must_use]
    pub fn or_memory_bytes(&self, memory_bytes: Option<usize>) -> Self {
        Self {
            memory_bytes: self.memory_bytes.or(memory_bytes),
            ..self.clone()
        }
    }

    #[must_use]
    pub fn has_any(&self) -> bool {
        self.num_cpus.is_some() || self.num_gpus.is_some() || self.memory_bytes.is_some()
    }

    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut requests = vec![];
        if let Some(num_cpus) = self.num_cpus {
            requests.push(format!("num_cpus = {num_cpus}"));
        }
        if let Some(num_gpus) = self.num_gpus {
            requests.push(format!("num_gpus = {num_gpus}"));
        }
        if let Some(memory_bytes) = self.memory_bytes {
            requests.push(format!("memory_bytes = {memory_bytes}"));
        }
        requests
    }

    /// Checks whether other is pipeline-compatible with self, i.e the resource requests are homogeneous enough that
    /// we don't want to pipeline tasks that have these resource requests with each other.
    ///
    /// Currently, this returns true unless one resource request has a non-zero CPU request and the other task has a
    /// non-zero GPU request.
    #[must_use]
    pub fn is_pipeline_compatible_with(&self, other: &Self) -> bool {
        let self_num_cpus = self.num_cpus;
        let self_num_gpus = self.num_gpus;
        let other_num_cpus = other.num_cpus;
        let other_num_gpus = other.num_gpus;
        match (self_num_cpus, self_num_gpus, other_num_cpus, other_num_gpus) {
            (_, Some(n_gpus), Some(n_cpus), _) | (Some(n_cpus), _, _, Some(n_gpus))
                if n_gpus > 0.0 && n_cpus > 0.0 =>
            {
                false
            }
            (_, _, _, _) => true,
        }
    }

    #[must_use]
    pub fn max(&self, other: &Self) -> Self {
        let max_num_cpus = lift(float_max, self.num_cpus, other.num_cpus);
        let max_num_gpus = lift(float_max, self.num_gpus, other.num_gpus);
        let max_memory_bytes = lift(std::cmp::max, self.memory_bytes, other.memory_bytes);
        Self::new_internal(max_num_cpus, max_num_gpus, max_memory_bytes)
    }

    pub fn max_all<ResourceRequestAsRef: AsRef<Self>>(
        resource_requests: &[ResourceRequestAsRef],
    ) -> Self {
        resource_requests
            .iter()
            .fold(Self::default(), |acc, e| acc.max(e.as_ref()))
    }

    #[must_use]
    pub fn multiply(&self, factor: f64) -> Self {
        Self::new_internal(
            self.num_cpus.map(|x| x * factor),
            self.num_gpus.map(|x| x * factor),
            self.memory_bytes.map(|x| x * (factor as usize)),
        )
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
        self.memory_bytes.hash(state);
    }
}

impl AsRef<Self> for ResourceRequest {
    fn as_ref(&self) -> &Self {
        self
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
    #[must_use]
    pub fn new(num_cpus: Option<f64>, num_gpus: Option<f64>, memory_bytes: Option<usize>) -> Self {
        Self::new_internal(num_cpus, num_gpus, memory_bytes)
    }

    /// Take a field-wise max of the list of resource requests.
    #[staticmethod]
    #[must_use]
    pub fn max_resources(resource_requests: Vec<Self>) -> Self {
        Self::max_all(&resource_requests.iter().collect::<Vec<_>>())
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

    #[must_use]
    pub fn with_num_cpus(&self, num_cpus: Option<f64>) -> Self {
        Self {
            num_cpus,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_num_gpus(&self, num_gpus: Option<f64>) -> Self {
        Self {
            num_gpus,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn with_memory_bytes(&self, memory_bytes: Option<usize>) -> Self {
        Self {
            memory_bytes,
            ..self.clone()
        }
    }

    fn __add__(&self, other: &Self) -> Self {
        self + other
    }

    fn __mul__(&self, factor: f64) -> Self {
        self.multiply(factor)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{self:?}"))
    }
}
impl_bincode_py_state_serialization!(ResourceRequest);

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<ResourceRequest>()?;
    Ok(())
}
