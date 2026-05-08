use daft_core::lit::Literal;

/// Pre-computed statistics exposed by a scan source for query optimization.
///
/// The optimizer uses these to populate `PlanStats` and make selectivity-aware
/// decisions without iterating scan tasks. Sources that know their totals cheaply
/// (manifest-backed formats like Iceberg/Delta, sized in-memory tables)
/// should return `Some` here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statistics {
    /// Total rows across the source.
    pub num_rows: Precision<u64>,
    /// On-disk size of the source in bytes.
    pub size_bytes: Precision<u64>,
    /// Per-column statistics, indexed by column position in the source schema.
    /// Empty when no column-level stats are available.
    pub column_statistics: Vec<ColumnStatistics>,
}

/// Per-column statistics. All fields are independently `Precision`-wrapped so
/// a source can report e.g. an exact `null_count` while leaving `distinct_count`
/// absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatistics {
    /// Number of nulls in the column.
    pub null_count: Precision<u64>,
    /// Minimum value in the column.
    pub min_value: Precision<Literal>,
    /// Maximum value in the column.
    pub max_value: Precision<Literal>,
    /// Number of distinct values in the column.
    pub distinct_count: Precision<u64>,
    /// Average or total on-disk byte size of the column; units are source-defined.
    pub size_bytes: Precision<u64>,
}

impl ColumnStatistics {
    /// Returns a `ColumnStatistics` with every field `Absent`.
    pub fn unknown() -> Self {
        Self {
            null_count: Precision::Absent,
            min_value: Precision::Absent,
            max_value: Precision::Absent,
            distinct_count: Precision::Absent,
            size_bytes: Precision::Absent,
        }
    }
}

/// Exactness annotation for a statistic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precision<T> {
    /// The value is exact. Safe to substitute for a full scan.
    Exact(T),
    /// The value is an estimate. Usable for heuristics, not substitution.
    Inexact(T),
    /// No value available.
    Absent,
}

impl<T> Precision<T> {
    /// Returns a reference to the inner value regardless of exactness, or `None` if absent.
    pub fn get(&self) -> Option<&T> {
        match self {
            Self::Exact(v) | Self::Inexact(v) => Some(v),
            Self::Absent => None,
        }
    }

    /// Returns the exact value, or `None` if inexact or absent.
    pub fn exact(&self) -> Option<&T> {
        match self {
            Self::Exact(v) => Some(v),
            _ => None,
        }
    }
}
