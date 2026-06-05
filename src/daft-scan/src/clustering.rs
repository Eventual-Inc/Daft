use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};

/// How a source declares its output is clustered at execution time, *without* the physical
/// partition count (which is only known once the source's scan tasks are enumerated).
///
/// This is the logical half of a clustering declaration: the scheme and its (possibly
/// expression-valued) keys. The planner attaches the partition count when it lowers the source,
/// producing a concrete clustering spec. Kept as an enum so non-hash schemes (e.g. range) can be
/// added without changing the `DataSource` API.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringKeys {
    /// Hash-partitioned by these keys: every row with the same hash of the keys lives in the same
    /// execution partition.
    Hash(Vec<ExprRef>),
    /// Each partition covers a non-overlapping range of values for the declared columns. No
    /// guarantee is made about the sort order of rows within each partition.
    Range(Vec<ExprRef>),
}

impl ClusteringKeys {
    pub fn hash(keys: Vec<ExprRef>) -> Self {
        Self::Hash(keys)
    }

    pub fn range(keys: Vec<ExprRef>) -> Self {
        Self::Range(keys)
    }

    /// The clustering key expressions.
    pub fn keys(&self) -> &[ExprRef] {
        match self {
            Self::Hash(keys) | Self::Range(keys) => keys,
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::prelude::*;

    use super::ClusteringKeys;

    /// Python handle for [`ClusteringKeys`], returned by a custom `DataSource`'s
    /// `get_clustering_keys()`.
    #[pyclass(module = "daft.daft", name = "ClusteringKeys", frozen, from_py_object)]
    #[derive(Debug, Clone)]
    pub struct PyClusteringKeys {
        pub keys: ClusteringKeys,
    }

    #[pymethods]
    impl PyClusteringKeys {
        #[staticmethod]
        fn hash(exprs: Vec<PyExpr>) -> Self {
            Self {
                keys: ClusteringKeys::Hash(exprs.into_iter().map(|e| e.expr).collect()),
            }
        }

        #[staticmethod]
        fn range(exprs: Vec<PyExpr>) -> Self {
            Self {
                keys: ClusteringKeys::Range(exprs.into_iter().map(|e| e.expr).collect()),
            }
        }

        fn __repr__(&self) -> String {
            let (variant, keys) = match &self.keys {
                ClusteringKeys::Hash(keys) => ("hash", keys),
                ClusteringKeys::Range(keys) => ("range", keys),
            };
            format!(
                "ClusteringKeys.{}({})",
                variant,
                keys.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }

    impl From<PyClusteringKeys> for ClusteringKeys {
        fn from(value: PyClusteringKeys) -> Self {
            value.keys
        }
    }
}

#[cfg(feature = "python")]
pub use python::PyClusteringKeys;
