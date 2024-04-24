use std::sync::Arc;

use daft_dsl::ExprRef;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Repartitioning specification.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RepartitionSpec {
    Hash(HashRepartitionConfig),
    Random(RandomShuffleConfig),
    IntoPartitions(IntoPartitionsConfig),
}

impl RepartitionSpec {
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Hash(_) => "Hash",
            Self::Random(_) => "Random",
            Self::IntoPartitions(_) => "IntoPartitions",
        }
    }

    pub fn repartition_by(&self) -> Vec<ExprRef> {
        match self {
            Self::Hash(HashRepartitionConfig { by, .. }) => by.clone(),
            _ => vec![],
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Hash(conf) => conf.multiline_display(),
            Self::Random(conf) => conf.multiline_display(),
            Self::IntoPartitions(conf) => conf.multiline_display(),
        }
    }

    pub fn to_clustering_spec(&self, upstream_num_partitions: usize) -> ClusteringSpec {
        match self {
            Self::Hash(HashRepartitionConfig { num_partitions, by }) => {
                ClusteringSpec::Hash(HashClusteringConfig::new(
                    num_partitions.unwrap_or(upstream_num_partitions),
                    by.clone(),
                ))
            }
            Self::Random(RandomShuffleConfig { num_partitions }) => ClusteringSpec::Random(
                RandomClusteringConfig::new(num_partitions.unwrap_or(upstream_num_partitions)),
            ),
            Self::IntoPartitions(IntoPartitionsConfig { num_partitions }) => {
                ClusteringSpec::Unknown(UnknownClusteringConfig::new(*num_partitions))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct HashRepartitionConfig {
    pub num_partitions: Option<usize>,
    pub by: Vec<ExprRef>,
}

impl HashRepartitionConfig {
    pub fn new(num_partitions: Option<usize>, by: Vec<ExprRef>) -> Self {
        Self { num_partitions, by }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Num partitions = {:?}", self.num_partitions));
        res.push(format!(
            "By = {}",
            self.by.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RandomShuffleConfig {
    pub num_partitions: Option<usize>,
}

impl RandomShuffleConfig {
    pub fn new(num_partitions: Option<usize>) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {:?}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct IntoPartitionsConfig {
    pub num_partitions: usize,
}

impl IntoPartitionsConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
    }
}

/// Partition scheme for Daft DataFrame.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringSpec {
    Range(RangeClusteringConfig),
    Hash(HashClusteringConfig),
    Random(RandomClusteringConfig),
    Unknown(UnknownClusteringConfig),
}

pub type ClusteringSpecRef = Arc<ClusteringSpec>;
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
            Self::Range(RangeClusteringConfig { num_partitions, .. }) => *num_partitions,
            Self::Hash(HashClusteringConfig { num_partitions, .. }) => *num_partitions,
            Self::Random(RandomClusteringConfig { num_partitions, .. }) => *num_partitions,
            Self::Unknown(UnknownClusteringConfig { num_partitions, .. }) => *num_partitions,
        }
    }

    pub fn partition_by(&self) -> Vec<ExprRef> {
        match self {
            Self::Range(RangeClusteringConfig { by, .. }) => by.clone(),
            Self::Hash(HashClusteringConfig { by, .. }) => by.clone(),
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
        Self::Unknown(UnknownClusteringConfig::new(1))
    }
}

impl From<RangeClusteringConfig> for ClusteringSpec {
    fn from(value: RangeClusteringConfig) -> Self {
        Self::Range(value)
    }
}

impl From<HashClusteringConfig> for ClusteringSpec {
    fn from(value: HashClusteringConfig) -> Self {
        Self::Hash(value)
    }
}

impl From<RandomClusteringConfig> for ClusteringSpec {
    fn from(value: RandomClusteringConfig) -> Self {
        Self::Random(value)
    }
}

impl From<UnknownClusteringConfig> for ClusteringSpec {
    fn from(value: UnknownClusteringConfig) -> Self {
        Self::Unknown(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RangeClusteringConfig {
    pub num_partitions: usize,
    pub by: Vec<ExprRef>,
    pub descending: Vec<bool>,
}

impl RangeClusteringConfig {
    pub fn new(num_partitions: usize, by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
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
pub struct HashClusteringConfig {
    pub num_partitions: usize,
    pub by: Vec<ExprRef>,
}

impl HashClusteringConfig {
    pub fn new(num_partitions: usize, by: Vec<ExprRef>) -> Self {
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
pub struct RandomClusteringConfig {
    num_partitions: usize,
}

impl RandomClusteringConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct UnknownClusteringConfig {
    num_partitions: usize,
}

impl UnknownClusteringConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
    }
}

impl Default for UnknownClusteringConfig {
    fn default() -> Self {
        UnknownClusteringConfig::new(1)
    }
}
