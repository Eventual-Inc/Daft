use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum StatsState {
    Materialized(PlanStats),
    NotMaterialized,
}

impl StatsState {
    pub fn unwrap_or_default(self) -> PlanStats {
        match self {
            Self::Materialized(stats) => stats,
            Self::NotMaterialized => PlanStats::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanStats {
    // Currently we're only putting cardinality stats in the plan stats.
    // In the future we want to start including column stats, including min, max, NDVs, etc.
    pub approx_stats: ApproxStats,
}

impl PlanStats {
    pub fn new(approx_stats: ApproxStats) -> Self {
        Self { approx_stats }
    }

    pub fn empty() -> Self {
        Self {
            approx_stats: ApproxStats::empty(),
        }
    }
}

impl Default for PlanStats {
    fn default() -> Self {
        Self::empty()
    }
}

impl Display for PlanStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ Lower bound rows = {}, Upper bound rows = {}, Lower bound bytes = {}, Upper bound bytes = {} }}",
            self.approx_stats.lower_bound_rows,
            self.approx_stats.upper_bound_rows.map_or("None".to_string(), |v| v.to_string()),
            self.approx_stats.lower_bound_bytes,
            self.approx_stats.upper_bound_bytes.map_or("None".to_string(), |v| v.to_string()),
        )
    }
}

// We implement PartialEq, Eq, and Hash so that all PlanStats are considered equal. This allows
// logical/physical plans that are enriched with stats to easily implement PartialEq, Eq, and Hash
// in a way that ignores PlanStats when considering equality.

impl PartialEq for PlanStats {
    #[inline]
    fn eq(&self, _other: &Self) -> bool {
        true // All PlanStats are considered equal.
    }
}

impl Eq for PlanStats {}

use std::hash::Hash;
impl Hash for PlanStats {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        // Add nothing to hash state since all PlanStats should hash the same.
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ApproxStats {
    pub lower_bound_rows: usize,
    pub upper_bound_rows: Option<usize>,
    pub lower_bound_bytes: usize,
    pub upper_bound_bytes: Option<usize>,
}

impl ApproxStats {
    pub fn empty() -> Self {
        Self {
            lower_bound_rows: 0,
            upper_bound_rows: None,
            lower_bound_bytes: 0,
            upper_bound_bytes: None,
        }
    }
    pub fn apply<F: Fn(usize) -> usize>(&self, f: F) -> Self {
        Self {
            lower_bound_rows: f(self.lower_bound_rows),
            upper_bound_rows: self.upper_bound_rows.map(&f),
            lower_bound_bytes: f(self.lower_bound_rows),
            upper_bound_bytes: self.upper_bound_bytes.map(&f),
        }
    }
}

use std::ops::Add;
impl Add for &ApproxStats {
    type Output = ApproxStats;
    fn add(self, rhs: Self) -> Self::Output {
        ApproxStats {
            lower_bound_rows: self.lower_bound_rows + rhs.lower_bound_rows,
            upper_bound_rows: self
                .upper_bound_rows
                .and_then(|l_ub| rhs.upper_bound_rows.map(|v| v + l_ub)),
            lower_bound_bytes: self.lower_bound_bytes + rhs.lower_bound_bytes,
            upper_bound_bytes: self
                .upper_bound_bytes
                .and_then(|l_ub| rhs.upper_bound_bytes.map(|v| v + l_ub)),
        }
    }
}
