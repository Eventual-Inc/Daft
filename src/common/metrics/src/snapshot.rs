use std::{collections::HashMap, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use enum_dispatch::enum_dispatch;
use indicatif::{HumanBytes, HumanCount};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    BYTES_IN_KEY, BYTES_OUT_KEY, BYTES_READ_KEY, BYTES_WRITTEN_KEY, DURATION_KEY,
    IN_MEMORY_BUFFER_BYTES_KEY, JOIN_BUILD_BYTES_INSERTED_KEY, JOIN_BUILD_ROWS_INSERTED_KEY,
    JOIN_PROBE_BYTES_IN_KEY, JOIN_PROBE_BYTES_OUT_KEY, JOIN_PROBE_ROWS_IN_KEY,
    JOIN_PROBE_ROWS_OUT_KEY, NUM_TASKS_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, ROWS_WRITTEN_KEY,
    SPILL_BYTES_READ_STAT_KEY, SPILL_BYTES_WRITTEN_STAT_KEY, SPILL_FILE_COUNT_STAT_KEY, Stat,
    Stats,
};

macro_rules! stats {
    ($($name:expr; $value:expr),* $(,)?) => {
        Stats(smallvec::smallvec![
            $( ($name.into(), $value) ),*
        ])
    };
}

#[enum_dispatch]
pub trait StatSnapshotImpl: Send + Sync + Serialize + Deserialize<'static> {
    fn duration_us(&self) -> u64;
    fn to_stats(&self) -> Stats;
    fn to_message(&self) -> String;
    /// Spill metrics, when the operator participates in spill accounting.
    /// Variant-agnostic: lets rollup code pick up spill from any snapshot
    /// without matching on the enum.
    fn spill_metrics(&self) -> Option<SpillSnapshot> {
        None
    }
    /// Current high-water in-memory buffer size (bytes), when the operator
    /// tracks one (blocking sinks that buffer before spilling). `None` when
    /// inapplicable.
    fn in_memory_buffer_bytes(&self) -> Option<u64> {
        None
    }
}

/// Source of spill I/O. Future variants (e.g. `RayObjectStore`) can be added
/// without schema churn since `SpillSnapshot` is optional on each snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum SpillSource {
    #[default]
    Native,
}

/// Per-operator spill metrics
///
/// attached to snapshot variants whose operators
/// can spill (shuffle, sort, aggregate, joins, ...). Absent on operators that
/// never spill; zero-valued reporters should emit `None` so the field is
/// skipped on serialization and doesn't light up the dashboard.
#[derive(Debug, Clone, Default, Encode, Decode, Serialize, Deserialize)]
pub struct SpillSnapshot {
    pub source: SpillSource,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub file_count: u64,
}

impl SpillSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        Self {
            source: self.source,
            bytes_written: self.bytes_written + other.bytes_written,
            bytes_read: self.bytes_read + other.bytes_read,
            file_count: self.file_count + other.file_count,
        }
    }
}

/// Flatten a `SpillSnapshot` into `Stats` entries so subscribers (Python,
/// dashboard, event-log) that consume the `to_stats()` key-value stream
/// pick up spill observability without special-casing each snapshot variant.
fn push_spill_stats(
    entries: &mut smallvec::SmallVec<[(Arc<str>, Stat); 7]>,
    spill: Option<&SpillSnapshot>,
) {
    let Some(spill) = spill else {
        return;
    };
    entries.push((
        SPILL_BYTES_WRITTEN_STAT_KEY.into(),
        Stat::Bytes(spill.bytes_written),
    ));
    entries.push((
        SPILL_BYTES_READ_STAT_KEY.into(),
        Stat::Bytes(spill.bytes_read),
    ));
    entries.push((
        SPILL_FILE_COUNT_STAT_KEY.into(),
        Stat::Count(spill.file_count),
    ));
}

fn merge_spill(a: Option<SpillSnapshot>, b: Option<&SpillSnapshot>) -> Option<SpillSnapshot> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.merge(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b.clone()),
        (None, None) => None,
    }
}

fn merge_buffer_bytes(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[derive(Debug, Clone, Default, Encode, Decode, Serialize, Deserialize)]
pub struct DefaultSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    #[serde(default)]
    pub bytes_in: u64,
    #[serde(default)]
    pub bytes_out: u64,
    #[serde(default)]
    pub num_tasks: u64,
    #[serde(default)]
    pub spill: Option<SpillSnapshot>,
    #[serde(default)]
    pub in_memory_buffer_bytes: Option<u64>,
}

impl StatSnapshotImpl for DefaultSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries: smallvec::SmallVec<[(Arc<str>, Stat); 7]> = smallvec::smallvec![
            (
                DURATION_KEY.into(),
                Stat::Duration(Duration::from_micros(self.cpu_us))
            ),
            (ROWS_IN_KEY.into(), Stat::Count(self.rows_in)),
            (ROWS_OUT_KEY.into(), Stat::Count(self.rows_out)),
            (BYTES_IN_KEY.into(), Stat::Bytes(self.bytes_in)),
            (BYTES_OUT_KEY.into(), Stat::Bytes(self.bytes_out)),
            (NUM_TASKS_KEY.into(), Stat::Count(self.num_tasks)),
        ];
        if let Some(buffer_bytes) = self.in_memory_buffer_bytes {
            entries.push((IN_MEMORY_BUFFER_BYTES_KEY.into(), Stat::Bytes(buffer_bytes)));
        }
        push_spill_stats(&mut entries, self.spill.as_ref());
        Stats(entries)
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows out",
            HumanCount(self.rows_in),
            HumanCount(self.rows_out),
        )
    }

    fn spill_metrics(&self) -> Option<SpillSnapshot> {
        self.spill.clone()
    }

    fn in_memory_buffer_bytes(&self) -> Option<u64> {
        self.in_memory_buffer_bytes
    }
}

impl DefaultSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            rows_in: self.rows_in + other.rows_in,
            rows_out: self.rows_out + other.rows_out,
            bytes_in: self.bytes_in + other.bytes_in,
            bytes_out: self.bytes_out + other.bytes_out,
            num_tasks: self.num_tasks + other.num_tasks,
            spill: merge_spill(self.spill, other.spill.as_ref()),
            in_memory_buffer_bytes: merge_buffer_bytes(
                self.in_memory_buffer_bytes,
                other.in_memory_buffer_bytes,
            ),
        }
    }
}

#[derive(Debug, Clone, Default, Encode, Decode, Serialize, Deserialize)]
pub struct SourceSnapshot {
    pub cpu_us: u64,
    pub rows_out: u64,
    pub bytes_read: u64,
    #[serde(default)]
    pub bytes_out: u64,
    #[serde(default)]
    pub num_tasks: u64,
    #[serde(default)]
    pub spill: Option<SpillSnapshot>,
}

impl StatSnapshotImpl for SourceSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries: smallvec::SmallVec<[(Arc<str>, Stat); 7]> = smallvec::smallvec![
            (
                DURATION_KEY.into(),
                Stat::Duration(Duration::from_micros(self.cpu_us))
            ),
            (ROWS_OUT_KEY.into(), Stat::Count(self.rows_out)),
            (BYTES_READ_KEY.into(), Stat::Bytes(self.bytes_read)),
            (BYTES_OUT_KEY.into(), Stat::Bytes(self.bytes_out)),
            (NUM_TASKS_KEY.into(), Stat::Count(self.num_tasks)),
        ];
        push_spill_stats(&mut entries, self.spill.as_ref());
        Stats(entries)
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows out, {} read",
            HumanCount(self.rows_out),
            HumanBytes(self.bytes_read),
        )
    }

    fn spill_metrics(&self) -> Option<SpillSnapshot> {
        self.spill.clone()
    }
}

impl SourceSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            rows_out: self.rows_out + other.rows_out,
            bytes_read: self.bytes_read + other.bytes_read,
            bytes_out: self.bytes_out + other.bytes_out,
            num_tasks: self.num_tasks + other.num_tasks,
            spill: merge_spill(self.spill, other.spill.as_ref()),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct FilterSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub selectivity: f64,
    #[serde(default)]
    pub bytes_in: u64,
    #[serde(default)]
    pub bytes_out: u64,
    #[serde(default)]
    pub num_tasks: u64,
}

impl StatSnapshotImpl for FilterSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        stats![
            DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            ROWS_IN_KEY; Stat::Count(self.rows_in),
            ROWS_OUT_KEY; Stat::Count(self.rows_out),
            "selectivity"; Stat::Percent(self.selectivity),
            BYTES_IN_KEY; Stat::Bytes(self.bytes_in),
            BYTES_OUT_KEY; Stat::Bytes(self.bytes_out),
            NUM_TASKS_KEY; Stat::Count(self.num_tasks),
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows out, {:.2}% kept",
            HumanCount(self.rows_in),
            HumanCount(self.rows_out),
            self.selectivity,
        )
    }
}

impl FilterSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        let rows_in = self.rows_in + other.rows_in;
        let rows_out = self.rows_out + other.rows_out;
        let selectivity = if rows_in > 0 {
            (rows_out as f64 / rows_in as f64) * 100.0
        } else {
            0.0
        };
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            rows_in,
            rows_out,
            selectivity,
            bytes_in: self.bytes_in + other.bytes_in,
            bytes_out: self.bytes_out + other.bytes_out,
            num_tasks: self.num_tasks + other.num_tasks,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ExplodeSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub amplification: f64,
    #[serde(default)]
    pub bytes_in: u64,
    #[serde(default)]
    pub bytes_out: u64,
    #[serde(default)]
    pub num_tasks: u64,
}

impl StatSnapshotImpl for ExplodeSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        stats![
            DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            ROWS_IN_KEY; Stat::Count(self.rows_in),
            ROWS_OUT_KEY; Stat::Count(self.rows_out),
            "amplification"; Stat::Float(self.amplification),
            BYTES_IN_KEY; Stat::Bytes(self.bytes_in),
            BYTES_OUT_KEY; Stat::Bytes(self.bytes_out),
            NUM_TASKS_KEY; Stat::Count(self.num_tasks),
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows out, {:.2}x inc",
            HumanCount(self.rows_in),
            HumanCount(self.rows_out),
            self.amplification,
        )
    }
}

impl ExplodeSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        let rows_in = self.rows_in + other.rows_in;
        let rows_out = self.rows_out + other.rows_out;
        let amplification = if rows_in > 0 {
            rows_out as f64 / rows_in as f64
        } else {
            0.0
        };
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            rows_in,
            rows_out,
            amplification,
            bytes_in: self.bytes_in + other.bytes_in,
            bytes_out: self.bytes_out + other.bytes_out,
            num_tasks: self.num_tasks + other.num_tasks,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct UdfSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub custom_counters: HashMap<Arc<str>, u64>,
    #[serde(default)]
    pub bytes_in: u64,
    #[serde(default)]
    pub bytes_out: u64,
    #[serde(default)]
    pub num_tasks: u64,
}

impl StatSnapshotImpl for UdfSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries = SmallVec::with_capacity(6 + self.custom_counters.len());

        entries.push((
            DURATION_KEY.into(),
            Stat::Duration(Duration::from_micros(self.cpu_us)),
        ));
        entries.push((ROWS_IN_KEY.into(), Stat::Count(self.rows_in)));
        entries.push((ROWS_OUT_KEY.into(), Stat::Count(self.rows_out)));
        entries.push((BYTES_IN_KEY.into(), Stat::Bytes(self.bytes_in)));
        entries.push((BYTES_OUT_KEY.into(), Stat::Bytes(self.bytes_out)));
        entries.push((NUM_TASKS_KEY.into(), Stat::Count(self.num_tasks)));

        for (name, value) in &self.custom_counters {
            entries.push((name.clone().into(), Stat::Count(*value)));
        }

        Stats(entries)
    }

    fn to_message(&self) -> String {
        if self.custom_counters.is_empty() {
            format!(
                "{} rows in, {} rows out",
                HumanCount(self.rows_in),
                HumanCount(self.rows_out),
            )
        } else {
            format!(
                "{} rows in, {} rows out, {}",
                HumanCount(self.rows_in),
                HumanCount(self.rows_out),
                self.custom_counters
                    .iter()
                    .map(|(name, value)| format!("{}: {}", name.as_ref(), HumanCount(*value)))
                    .join(", ")
            )
        }
    }
}

impl UdfSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        let mut custom_counters = self.custom_counters;
        for (k, v) in &other.custom_counters {
            *custom_counters.entry(k.clone()).or_insert(0) += v;
        }
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            rows_in: self.rows_in + other.rows_in,
            rows_out: self.rows_out + other.rows_out,
            custom_counters,
            bytes_in: self.bytes_in + other.bytes_in,
            bytes_out: self.bytes_out + other.bytes_out,
            num_tasks: self.num_tasks + other.num_tasks,
        }
    }
}

#[derive(Debug, Clone, Default, Encode, Decode, Serialize, Deserialize)]
pub struct JoinSnapshot {
    pub cpu_us: u64,
    pub build_rows_inserted: u64,
    pub probe_rows_in: u64,
    pub probe_rows_out: u64,
    #[serde(default)]
    pub build_bytes_inserted: u64,
    #[serde(default)]
    pub probe_bytes_in: u64,
    #[serde(default)]
    pub probe_bytes_out: u64,
    #[serde(default)]
    pub num_tasks: u64,
    #[serde(default)]
    pub spill: Option<SpillSnapshot>,
}

impl StatSnapshotImpl for JoinSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries: smallvec::SmallVec<[(Arc<str>, Stat); 7]> = smallvec::smallvec![
            (
                DURATION_KEY.into(),
                Stat::Duration(Duration::from_micros(self.cpu_us))
            ),
            (
                JOIN_BUILD_ROWS_INSERTED_KEY.into(),
                Stat::Count(self.build_rows_inserted)
            ),
            (
                JOIN_PROBE_ROWS_IN_KEY.into(),
                Stat::Count(self.probe_rows_in)
            ),
            (
                JOIN_PROBE_ROWS_OUT_KEY.into(),
                Stat::Count(self.probe_rows_out)
            ),
            (
                JOIN_BUILD_BYTES_INSERTED_KEY.into(),
                Stat::Bytes(self.build_bytes_inserted)
            ),
            (
                JOIN_PROBE_BYTES_IN_KEY.into(),
                Stat::Bytes(self.probe_bytes_in)
            ),
            (
                JOIN_PROBE_BYTES_OUT_KEY.into(),
                Stat::Bytes(self.probe_bytes_out)
            ),
            (NUM_TASKS_KEY.into(), Stat::Count(self.num_tasks)),
        ];
        push_spill_stats(&mut entries, self.spill.as_ref());
        Stats(entries)
    }

    fn to_message(&self) -> String {
        format!(
            "{} build rows inserted, {} probe rows in, {} probe rows out",
            HumanCount(self.build_rows_inserted),
            HumanCount(self.probe_rows_in),
            HumanCount(self.probe_rows_out),
        )
    }

    fn spill_metrics(&self) -> Option<SpillSnapshot> {
        self.spill.clone()
    }
}

impl JoinSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            build_rows_inserted: self.build_rows_inserted + other.build_rows_inserted,
            probe_rows_in: self.probe_rows_in + other.probe_rows_in,
            probe_rows_out: self.probe_rows_out + other.probe_rows_out,
            build_bytes_inserted: self.build_bytes_inserted + other.build_bytes_inserted,
            probe_bytes_in: self.probe_bytes_in + other.probe_bytes_in,
            probe_bytes_out: self.probe_bytes_out + other.probe_bytes_out,
            num_tasks: self.num_tasks + other.num_tasks,
            spill: merge_spill(self.spill, other.spill.as_ref()),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct WriteSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    #[serde(default)]
    pub bytes_in: u64,
    #[serde(default)]
    pub num_tasks: u64,
}

impl StatSnapshotImpl for WriteSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        stats![
            DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            ROWS_IN_KEY; Stat::Count(self.rows_in),
            ROWS_WRITTEN_KEY; Stat::Count(self.rows_written),
            BYTES_WRITTEN_KEY; Stat::Bytes(self.bytes_written),
            BYTES_IN_KEY; Stat::Bytes(self.bytes_in),
            NUM_TASKS_KEY; Stat::Count(self.num_tasks),
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows written, {} written",
            HumanCount(self.rows_in),
            HumanCount(self.rows_written),
            HumanBytes(self.bytes_written),
        )
    }
}

impl WriteSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            rows_in: self.rows_in + other.rows_in,
            rows_written: self.rows_written + other.rows_written,
            bytes_written: self.bytes_written + other.bytes_written,
            bytes_in: self.bytes_in + other.bytes_in,
            num_tasks: self.num_tasks + other.num_tasks,
        }
    }
}

#[enum_dispatch(StatSnapshotImpl)]
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub enum StatSnapshot {
    Default(DefaultSnapshot),
    Source(SourceSnapshot),
    Filter(FilterSnapshot),
    Explode(ExplodeSnapshot),
    Udf(UdfSnapshot),
    Join(JoinSnapshot),
    Write(WriteSnapshot),
}

impl StatSnapshot {
    /// Sum two same-variant snapshots. Mismatched variants return `self` unchanged.
    pub fn merge(self, other: &Self) -> Self {
        match (self, other) {
            (Self::Default(a), Self::Default(b)) => Self::Default(a.merge(b)),
            (Self::Source(a), Self::Source(b)) => Self::Source(a.merge(b)),
            (Self::Filter(a), Self::Filter(b)) => Self::Filter(a.merge(b)),
            (Self::Explode(a), Self::Explode(b)) => Self::Explode(a.merge(b)),
            (Self::Udf(a), Self::Udf(b)) => Self::Udf(a.merge(b)),
            (Self::Join(a), Self::Join(b)) => Self::Join(a.merge(b)),
            (Self::Write(a), Self::Write(b)) => Self::Write(a.merge(b)),
            (s, _) => s,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NUM_TASKS_KEY, Stat};

    fn num_tasks_stat(snapshot: &StatSnapshot) -> u64 {
        snapshot
            .to_stats()
            .iter()
            .find_map(|(name, stat)| {
                (name == NUM_TASKS_KEY).then(|| match stat {
                    Stat::Count(v) => *v,
                    other => panic!("expected Count for num_tasks, got {:?}", other),
                })
            })
            .expect("num_tasks key should be present in to_stats output")
    }

    #[test]
    fn merge_sums_num_tasks_for_every_variant() {
        // (left, right, expected_num_tasks)
        let cases: Vec<(StatSnapshot, StatSnapshot, u64)> = vec![
            (
                StatSnapshot::Default(DefaultSnapshot {
                    num_tasks: 2,
                    ..Default::default()
                }),
                StatSnapshot::Default(DefaultSnapshot {
                    num_tasks: 5,
                    ..Default::default()
                }),
                7,
            ),
            (
                StatSnapshot::Source(SourceSnapshot {
                    num_tasks: 1,
                    ..Default::default()
                }),
                StatSnapshot::Source(SourceSnapshot {
                    num_tasks: 3,
                    ..Default::default()
                }),
                4,
            ),
            (
                StatSnapshot::Filter(FilterSnapshot {
                    num_tasks: 4,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_out: 0,
                    selectivity: 0.0,
                    bytes_in: 0,
                    bytes_out: 0,
                }),
                StatSnapshot::Filter(FilterSnapshot {
                    num_tasks: 6,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_out: 0,
                    selectivity: 0.0,
                    bytes_in: 0,
                    bytes_out: 0,
                }),
                10,
            ),
            (
                StatSnapshot::Explode(ExplodeSnapshot {
                    num_tasks: 2,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_out: 0,
                    amplification: 0.0,
                    bytes_in: 0,
                    bytes_out: 0,
                }),
                StatSnapshot::Explode(ExplodeSnapshot {
                    num_tasks: 2,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_out: 0,
                    amplification: 0.0,
                    bytes_in: 0,
                    bytes_out: 0,
                }),
                4,
            ),
            (
                StatSnapshot::Udf(UdfSnapshot {
                    num_tasks: 1,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_out: 0,
                    custom_counters: HashMap::new(),
                    bytes_in: 0,
                    bytes_out: 0,
                }),
                StatSnapshot::Udf(UdfSnapshot {
                    num_tasks: 9,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_out: 0,
                    custom_counters: HashMap::new(),
                    bytes_in: 0,
                    bytes_out: 0,
                }),
                10,
            ),
            (
                StatSnapshot::Join(JoinSnapshot {
                    num_tasks: 3,
                    ..Default::default()
                }),
                StatSnapshot::Join(JoinSnapshot {
                    num_tasks: 8,
                    ..Default::default()
                }),
                11,
            ),
            (
                StatSnapshot::Write(WriteSnapshot {
                    num_tasks: 1,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_written: 0,
                    bytes_written: 0,
                    bytes_in: 0,
                }),
                StatSnapshot::Write(WriteSnapshot {
                    num_tasks: 2,
                    cpu_us: 0,
                    rows_in: 0,
                    rows_written: 0,
                    bytes_written: 0,
                    bytes_in: 0,
                }),
                3,
            ),
        ];

        for (a, b, expected) in cases {
            let merged = a.merge(&b);
            assert_eq!(
                num_tasks_stat(&merged),
                expected,
                "merged snapshot: {merged:?}"
            );
        }
    }
}
