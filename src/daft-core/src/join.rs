use std::{ops::Not, str::FromStr};

use common_error::{DaftError, DaftResult};
use common_py_serde::impl_bincode_py_state_serialization;
use derive_more::Display;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyObject, PyResult, Python};
use serde::{Deserialize, Serialize};

/// Type of a join operation.
#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
    Anti,
    Semi,
}

#[cfg(feature = "python")]
#[pymethods]
impl JoinType {
    /// Create a JoinType from its string representation.
    ///
    /// Args:
    ///     join_type: String representation of the join type, e.g. "inner", "left", or "right".
    #[staticmethod]
    pub fn from_join_type_str(join_type: &str) -> PyResult<Self> {
        Self::from_str(join_type).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}
impl_bincode_py_state_serialization!(JoinType);

impl JoinType {
    pub fn iterator() -> std::slice::Iter<'static, Self> {
        static JOIN_TYPES: [JoinType; 6] = [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Outer,
            JoinType::Anti,
            JoinType::Semi,
        ];
        JOIN_TYPES.iter()
    }

    pub fn left_produces_nulls(&self) -> bool {
        match self {
            Self::Right | Self::Outer => true,
            Self::Inner | Self::Left | Self::Anti | Self::Semi => false,
        }
    }

    pub fn right_produces_nulls(&self) -> bool {
        match self {
            Self::Left | Self::Outer => true,
            Self::Inner | Self::Right | Self::Anti | Self::Semi => false,
        }
    }
}

impl FromStr for JoinType {
    type Err = DaftError;

    fn from_str(join_type: &str) -> DaftResult<Self> {
        match join_type {
            "inner" => Ok(Self::Inner),
            "cross" => Ok(Self::Inner), // cross join is just inner join with no join keys
            "left" => Ok(Self::Left),
            "right" => Ok(Self::Right),
            "outer" => Ok(Self::Outer),
            "anti" => Ok(Self::Anti),
            "semi" => Ok(Self::Semi),
            _ => Err(DaftError::TypeError(format!(
                "Join type {} is not supported; only the following types are supported: {:?}",
                join_type,
                Self::iterator().as_slice()
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum JoinStrategy {
    Hash,
    SortMerge,
    Broadcast,
    /// only used internally, do not let users to specify
    Cross,
}

#[cfg(feature = "python")]
#[pymethods]
impl JoinStrategy {
    /// Create a JoinStrategy from its string representation.
    ///
    /// Args:
    ///     join_strategy: String representation of the join strategy, e.g. "hash", "sort_merge", or "broadcast".
    #[staticmethod]
    pub fn from_join_strategy_str(join_strategy: &str) -> PyResult<Self> {
        Self::from_str(join_strategy).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(self.to_string())
    }
}
impl_bincode_py_state_serialization!(JoinStrategy);

impl JoinStrategy {
    pub fn iterator() -> std::slice::Iter<'static, Self> {
        static JOIN_STRATEGIES: [JoinStrategy; 3] = [
            JoinStrategy::Hash,
            JoinStrategy::SortMerge,
            JoinStrategy::Broadcast,
        ];
        JOIN_STRATEGIES.iter()
    }
}

impl FromStr for JoinStrategy {
    type Err = DaftError;

    fn from_str(join_strategy: &str) -> DaftResult<Self> {
        match join_strategy {
            "hash" => Ok(Self::Hash),
            "sort_merge" => Ok(Self::SortMerge),
            "broadcast" => Ok(Self::Broadcast),
            _ => Err(DaftError::TypeError(format!(
                "Join strategy {} is not supported; only the following strategies are supported: {:?}",
                join_strategy,
                Self::iterator().as_slice()
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, eq_int))]
pub enum JoinSide {
    #[display("left")]
    Left,
    #[display("right")]
    Right,
}

impl Not for JoinSide {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

impl_bincode_py_state_serialization!(JoinSide);
