use std::{fmt::Display, hash::Hash, ops::Deref};

use common_display::utils::bytes_to_human_readable;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum StatsState {
    Materialized(AlwaysSame<PlanStats>),
    NotMaterialized,
}

impl StatsState {
    pub fn materialized_stats(&self) -> &PlanStats {
        match self {
            Self::Materialized(stats) => stats,
            Self::NotMaterialized => panic!("Tried to get unmaterialized stats"),
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
        use num_format::{Locale, ToFormattedString};
        write!(
            f,
            "{{ Approx num rows = {}, Approx size bytes = {} }}",
            self.approx_stats.num_rows.to_formatted_string(&Locale::en),
            bytes_to_human_readable(self.approx_stats.size_bytes),
        )
    }
}

// We implement PartialEq, Eq, and Hash for AlwaysSame, then add PlanStats to LogicalPlans wrapped by AlwaysSame.
// This allows all PlanStats to be considered equal, so that logical/physical plans that are enriched with
// stats can easily implement PartialEq, Eq, and Hash in a way that ignores PlanStats when considering equality.

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AlwaysSame<T>(T);

impl<T> Deref for AlwaysSame<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Hash for AlwaysSame<T> {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        // Add nothing to hash state since all AlwaysSame should hash the same.
    }
}

impl<T> Eq for AlwaysSame<T> {}

impl<T> PartialEq for AlwaysSame<T> {
    #[inline]
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<T> From<T> for AlwaysSame<T> {
    #[inline]
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T: Display> Display for AlwaysSame<T> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ApproxStats {
    pub num_rows: usize,
    pub size_bytes: usize,
}

impl ApproxStats {
    pub fn empty() -> Self {
        Self {
            num_rows: 0,
            size_bytes: 0,
        }
    }
    pub fn apply<F: Fn(usize) -> usize>(&self, f: F) -> Self {
        Self {
            num_rows: f(self.num_rows),
            size_bytes: f(self.size_bytes),
        }
    }
}

use std::ops::Add;
impl Add for &ApproxStats {
    type Output = ApproxStats;
    fn add(self, rhs: Self) -> Self::Output {
        ApproxStats {
            num_rows: self.num_rows + rhs.num_rows,
            size_bytes: self.size_bytes + rhs.size_bytes,
        }
    }
}
