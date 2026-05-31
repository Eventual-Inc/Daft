use std::{fmt::Display, sync::Arc};

use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Repartitioning specification.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RepartitionSpec {
    Hash(HashRepartitionConfig),
    Random(RandomShuffleConfig),
    Range(RangeRepartitionConfig),
}

impl RepartitionSpec {
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Hash(_) => "Hash",
            Self::Random(_) => "Random",
            Self::Range(_) => "Range",
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
            Self::Range(conf) => conf.multiline_display(),
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
            Self::Random(RandomShuffleConfig { num_partitions, .. }) => ClusteringSpec::Random(
                RandomClusteringConfig::new(num_partitions.unwrap_or(upstream_num_partitions)),
            ),
            Self::Range(RangeRepartitionConfig {
                num_partitions,
                by,
                descending,
                ..
            }) => ClusteringSpec::Range(RangeClusteringConfig::new(
                num_partitions.unwrap_or(upstream_num_partitions),
                by.iter().map(|e| e.inner().clone()).collect(),
                descending.clone(),
            )),
        }
    }

    pub fn compact_display(&self) -> String {
        fn format_num_partitions(num_partitions: Option<usize>) -> String {
            num_partitions
                .map(|value| value.to_string())
                .unwrap_or_else(|| "auto".to_string())
        }

        fn format_list<T: Display>(items: impl IntoIterator<Item = T>) -> String {
            format!(
                "[{}]",
                items.into_iter().map(|item| item.to_string()).join(", ")
            )
        }

        match self {
            Self::Hash(HashRepartitionConfig { num_partitions, by }) => format!(
                "Hash (num_partitions={}, by={})",
                format_num_partitions(*num_partitions),
                format_list(by.iter().map(|expr| expr.to_string()))
            ),
            Self::Random(RandomShuffleConfig {
                num_partitions,
                seed,
            }) => {
                let mut parts = vec![format!(
                    "num_partitions={}",
                    format_num_partitions(*num_partitions)
                )];
                if let Some(seed) = seed {
                    parts.push(format!("seed={seed}"));
                }
                format!("Random ({})", parts.join(", "))
            }
            Self::Range(RangeRepartitionConfig {
                num_partitions,
                by,
                descending,
                ..
            }) => {
                let mut parts = vec![
                    format!("num_partitions={}", format_num_partitions(*num_partitions)),
                    format!("by={}", format_list(by.iter().map(|expr| expr.to_string()))),
                ];

                if !descending.is_empty() {
                    parts.push(format!("descending={}", format_list(descending.iter())));
                }
                format!("Range ({})", parts.join(", "))
            }
        }
    }
}

impl Display for RepartitionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.compact_display())
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
    pub seed: Option<u64>,
}

impl RandomShuffleConfig {
    pub fn new(num_partitions: Option<usize>) -> Self {
        Self {
            num_partitions,
            seed: None,
        }
    }

    pub fn new_with_seed(num_partitions: Option<usize>, seed: Option<u64>) -> Self {
        Self {
            num_partitions,
            seed,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {:?}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RangeRepartitionConfig {
    pub num_partitions: Option<usize>,
    pub boundaries: RecordBatch,
    pub by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
}

impl RangeRepartitionConfig {
    pub fn new(
        num_partitions: Option<usize>,
        boundaries: RecordBatch,
        by: Vec<BoundExpr>,
        descending: Vec<bool>,
    ) -> Self {
        Self {
            num_partitions,
            boundaries,
            by,
            descending,
        }
    }
}

impl RangeRepartitionConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        let pairs = self
            .by
            .iter()
            .zip(self.descending.iter())
            .map(|(sb, d)| format!("({}, {})", sb, if *d { "descending" } else { "ascending" },))
            .join(", ");
        res.push(format!("Num partitions = {:?}", self.num_partitions));
        res.push(format!("By = {}", pairs));
        res
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

    pub fn unknown() -> Self {
        Self::Unknown(UnknownClusteringConfig::new(0))
    }

    pub fn unknown_with_num_partitions(num_partitions: usize) -> Self {
        Self::Unknown(UnknownClusteringConfig::new(num_partitions))
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

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
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
        Self::new(1)
    }
}
