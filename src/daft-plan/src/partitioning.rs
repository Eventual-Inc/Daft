use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use daft_dsl::Expr;

use daft_core::impl_bincode_py_state_serialization;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{
        pyclass, pyclass::CompareOp, pymethods, types::PyBytes, IntoPy, PyObject, PyResult,
        PyTypeInfo, Python, ToPyObject,
    },
};

/// Partition scheme for Daft DataFrame.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum PartitionScheme {
    Range,
    Hash,
    Random,
    Unknown,
}

impl_bincode_py_state_serialization!(PartitionScheme);

impl Display for PartitionScheme {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        // Leverage Debug trait implementation, which will already return the enum variant as a string.
        write!(f, "{:?}", self)
    }
}

/// Partition scheme for Daft DataFrame.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitionSchemeConfig {
    Range(RangeConfig),
    Hash(HashConfig),
    Random(RandomConfig),
    Unknown(UnknownConfig),
}

impl PartitionSchemeConfig {
    pub fn from_scheme(&self, scheme: PartitionScheme) -> Self {
        match scheme {
            PartitionScheme::Range => Self::Range(Default::default()),
            PartitionScheme::Hash => Self::Hash(Default::default()),
            PartitionScheme::Random => Self::Random(Default::default()),
            PartitionScheme::Unknown => Self::Unknown(Default::default()),
        }
    }
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Range(_) => "Range",
            Self::Hash(_) => "Hash",
            Self::Random(_) => "Random",
            Self::Unknown(_) => "Unknown",
        }
    }

    pub fn to_scheme(&self) -> PartitionScheme {
        match self {
            Self::Range(_) => PartitionScheme::Range,
            Self::Hash(_) => PartitionScheme::Hash,
            Self::Random(_) => PartitionScheme::Random,
            Self::Unknown(_) => PartitionScheme::Unknown,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Range(conf) => conf.multiline_display(),
            Self::Hash(conf) => conf.multiline_display(),
            Self::Random(conf) => conf.multiline_display(),
            Self::Unknown(conf) => conf.multiline_display(),
        }
    }
}

impl Default for PartitionSchemeConfig {
    fn default() -> Self {
        Self::Unknown(Default::default())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct RangeConfig {}

impl RangeConfig {
    pub fn new_internal() -> Self {
        Self {}
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![]
    }
}

impl Default for RangeConfig {
    fn default() -> Self {
        Self::new_internal()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl RangeConfig {
    /// Create a config for range partitioning.
    #[new]
    fn new() -> Self {
        Self::new_internal()
    }
}
impl_bincode_py_state_serialization!(RangeConfig);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct HashConfig {}

impl HashConfig {
    pub fn new_internal() -> Self {
        Self {}
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![]
    }
}

impl Default for HashConfig {
    fn default() -> Self {
        Self::new_internal()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl HashConfig {
    /// Create a config for hash partitioning.
    #[new]
    fn new() -> Self {
        Self::new_internal()
    }
}

impl_bincode_py_state_serialization!(HashConfig);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct RandomConfig {}

impl RandomConfig {
    pub fn new_internal() -> Self {
        Self {}
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![]
    }
}

impl Default for RandomConfig {
    fn default() -> Self {
        Self::new_internal()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl RandomConfig {
    /// Create a config for random partitioning.
    #[new]
    fn new() -> Self {
        Self::new_internal()
    }
}

impl_bincode_py_state_serialization!(RandomConfig);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct UnknownConfig {}

impl UnknownConfig {
    pub fn new_internal() -> Self {
        Self {}
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![]
    }
}

impl Default for UnknownConfig {
    fn default() -> Self {
        Self::new_internal()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl UnknownConfig {
    /// Create a config for unknown partitioning.
    #[new]
    fn new() -> Self {
        Self::new_internal()
    }
}

impl_bincode_py_state_serialization!(UnknownConfig);

/// Partition specification: scheme config, number of partitions, partition column.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub scheme_config: PartitionSchemeConfig,
    pub num_partitions: usize,
    pub by: Option<Vec<Expr>>,
}

impl PartitionSpec {
    pub fn new(
        scheme_config: PartitionSchemeConfig,
        num_partitions: usize,
        by: Option<Vec<Expr>>,
    ) -> Self {
        Self {
            scheme_config,
            num_partitions,
            by,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Scheme = {}", self.scheme_config.to_scheme()));
        let scheme_config = self.scheme_config.multiline_display();
        if !scheme_config.is_empty() {
            res.push(format!(
                "{} config = {}",
                self.scheme_config.var_name(),
                scheme_config.join(", ")
            ));
        }
        res.push(format!("Num partitions = {}", self.num_partitions));
        if let Some(ref by) = self.by {
            res.push(format!(
                "By = {}",
                by.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        res
    }
}

impl Default for PartitionSpec {
    fn default() -> Self {
        Self::new(PartitionSchemeConfig::Unknown(Default::default()), 1, None)
    }
}

/// Configuration for parsing a particular file format.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "PartitionSpec")
)]
pub struct PyPartitionSpec(Arc<PartitionSpec>);

#[cfg(feature = "python")]
#[pymethods]
impl PyPartitionSpec {
    /// Create a range partitioning spec.
    #[staticmethod]
    #[pyo3(signature = (scheme_config=RangeConfig::default(), num_partitions=0usize, by=None))]
    fn range(scheme_config: RangeConfig, num_partitions: usize, by: Option<Vec<PyExpr>>) -> Self {
        Self(
            PartitionSpec::new(
                PartitionSchemeConfig::Range(scheme_config),
                num_partitions,
                by.map(|v| v.iter().map(|e| e.clone().into()).collect()),
            )
            .into(),
        )
    }

    /// Create a hash partitioning spec.
    #[staticmethod]
    #[pyo3(signature = (scheme_config=Default::default(), num_partitions=0usize, by=None))]
    fn hash(scheme_config: HashConfig, num_partitions: usize, by: Option<Vec<PyExpr>>) -> Self {
        Self(
            PartitionSpec::new(
                PartitionSchemeConfig::Hash(scheme_config),
                num_partitions,
                by.map(|v| v.iter().map(|e| e.clone().into()).collect()),
            )
            .into(),
        )
    }

    /// Create a random partitioning spec.
    #[staticmethod]
    #[pyo3(signature = (scheme_config=Default::default(), num_partitions=0usize, by=None))]
    fn random(scheme_config: RandomConfig, num_partitions: usize, by: Option<Vec<PyExpr>>) -> Self {
        Self(
            PartitionSpec::new(
                PartitionSchemeConfig::Random(scheme_config),
                num_partitions,
                by.map(|v| v.iter().map(|e| e.clone().into()).collect()),
            )
            .into(),
        )
    }

    /// Create a unknown partitioning spec.
    #[staticmethod]
    #[pyo3(signature = (scheme_config=Default::default(), num_partitions=0usize, by=None))]
    fn unknown(
        scheme_config: UnknownConfig,
        num_partitions: usize,
        by: Option<Vec<PyExpr>>,
    ) -> Self {
        Self(
            PartitionSpec::new(
                PartitionSchemeConfig::Unknown(scheme_config),
                num_partitions,
                by.map(|v| v.iter().map(|e| e.clone().into()).collect()),
            )
            .into(),
        )
    }

    /// Get the underlying partitioning scheme config.
    #[getter]
    fn get_config(&self, py: Python) -> PyObject {
        use PartitionSchemeConfig::*;

        match &self.0.scheme_config {
            Range(config) => config.clone().into_py(py),
            Hash(config) => config.clone().into_py(py),
            Random(config) => config.clone().into_py(py),
            Unknown(config) => config.clone().into_py(py),
        }
    }

    #[getter]
    pub fn get_scheme(&self) -> PyResult<PartitionScheme> {
        Ok(self.0.scheme_config.to_scheme())
    }

    #[getter]
    pub fn get_num_partitions(&self) -> PyResult<i64> {
        Ok(self.0.num_partitions as i64)
    }

    #[getter]
    pub fn get_by(&self) -> PyResult<Option<Vec<PyExpr>>> {
        Ok(self
            .0
            .by
            .as_ref()
            .map(|v| v.iter().map(|e| e.clone().into()).collect()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.0))
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self.0 == other.0,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }
}

impl_bincode_py_state_serialization!(PyPartitionSpec);

impl From<PyPartitionSpec> for Arc<PartitionSpec> {
    fn from(partition_spec: PyPartitionSpec) -> Self {
        partition_spec.0
    }
}

impl From<Arc<PartitionSpec>> for PyPartitionSpec {
    fn from(partition_spec: Arc<PartitionSpec>) -> Self {
        Self(partition_spec)
    }
}
