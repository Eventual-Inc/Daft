use daft_dsl::Expr;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Partition scheme for Daft DataFrame.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringSpec {
    Range(RangePartitioningConfig),
    Hash(HashPartitioningConfig),
    Random(RandomPartitioningConfig),
    Unknown(UnknownPartitioningConfig),
}

impl ClusteringSpec {
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Range(_) => "Range",
            Self::Hash(_) => "Hash",
            Self::Random(_) => "Random",
            Self::Unknown(_) => "Unknown",
        }
    }

    pub fn num_partitions(&self) -> usize {
        match self {
            Self::Range(RangePartitioningConfig { num_partitions, .. }) => *num_partitions,
            Self::Hash(HashPartitioningConfig { num_partitions, .. }) => *num_partitions,
            Self::Random(RandomPartitioningConfig { num_partitions, .. }) => *num_partitions,
            Self::Unknown(UnknownPartitioningConfig { num_partitions, .. }) => *num_partitions,
        }
    }

    pub fn partition_by(&self) -> Vec<Expr> {
        match self {
            Self::Range(RangePartitioningConfig { by, .. }) => by.clone(),
            Self::Hash(HashPartitioningConfig { by, .. }) => by.clone(),
            _ => vec![],
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

impl Default for ClusteringSpec {
    fn default() -> Self {
        Self::Unknown(UnknownPartitioningConfig::new(1))
    }
}

impl From<RangePartitioningConfig> for ClusteringSpec {
    fn from(value: RangePartitioningConfig) -> Self {
        Self::Range(value)
    }
}

impl From<HashPartitioningConfig> for ClusteringSpec {
    fn from(value: HashPartitioningConfig) -> Self {
        Self::Hash(value)
    }
}

impl From<RandomPartitioningConfig> for ClusteringSpec {
    fn from(value: RandomPartitioningConfig) -> Self {
        Self::Random(value)
    }
}

impl From<UnknownPartitioningConfig> for ClusteringSpec {
    fn from(value: UnknownPartitioningConfig) -> Self {
        Self::Unknown(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RangePartitioningConfig {
    pub num_partitions: usize,
    pub by: Vec<Expr>,
    pub descending: Vec<bool>,
}

impl RangePartitioningConfig {
    pub fn new(num_partitions: usize, by: Vec<Expr>, descending: Vec<bool>) -> Self {
        Self {
            num_partitions,
            by,
            descending,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        let pairs = self
            .by
            .iter()
            .zip(self.descending.iter())
            .map(|(sb, d)| format!("({}, {})", sb, if *d { "descending" } else { "ascending" },))
            .join(", ");
        res.push(format!("Num partitions = {}", self.num_partitions));
        res.push(format!("By = {}", pairs));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct HashPartitioningConfig {
    pub num_partitions: usize,
    pub by: Vec<Expr>,
}

impl HashPartitioningConfig {
    pub fn new(num_partitions: usize, by: Vec<Expr>) -> Self {
        Self { num_partitions, by }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Num partitions = {}", self.num_partitions));
        res.push(format!(
            "By = {}",
            self.by.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RandomPartitioningConfig {
    num_partitions: usize,
}

impl RandomPartitioningConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct UnknownPartitioningConfig {
    num_partitions: usize,
}

impl UnknownPartitioningConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
    }
}

impl Default for UnknownPartitioningConfig {
    fn default() -> Self {
        UnknownPartitioningConfig::new(1)
    }
}
