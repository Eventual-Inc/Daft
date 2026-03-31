use std::{borrow::Cow, collections::HashMap, fmt::Write, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use enum_dispatch::enum_dispatch;
use indicatif::{HumanBytes, HumanCount};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    BYTES_READ_KEY, BYTES_WRITTEN_KEY, DURATION_KEY, ESTIMATED_TOTAL_PROBE_ROWS_KEY,
    ESTIMATED_TOTAL_ROWS_KEY, JOIN_BUILD_ROWS_INSERTED_KEY, JOIN_PROBE_ROWS_IN_KEY,
    JOIN_PROBE_ROWS_OUT_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, ROWS_WRITTEN_KEY, Stat, Stats,
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
    /// Render message for progress bars
    fn to_message(&self) -> Cow<'static, str>;
    /// Current progress of the operator
    fn current_progress(&self) -> u64;
    /// "Total" count of the operator
    fn total(&self) -> u64;
    /// Metric name for estimated operator progress
    fn total_key(&self) -> &'static str;
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct DefaultSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub estimated_total_rows: u64,
}

impl StatSnapshotImpl for DefaultSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        stats![
            DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            ROWS_IN_KEY; Stat::Count(self.rows_in),
            ROWS_OUT_KEY; Stat::Count(self.rows_out),
            ESTIMATED_TOTAL_ROWS_KEY; Stat::Count(self.estimated_total_rows),
        ]
    }

    fn to_message(&self) -> Cow<'static, str> {
        Cow::Borrowed("")
    }

    fn current_progress(&self) -> u64 {
        self.rows_out
    }

    fn total(&self) -> u64 {
        self.estimated_total_rows
    }

    fn total_key(&self) -> &'static str {
        "rows out"
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SourceSnapshot {
    pub cpu_us: u64,
    pub rows_out: u64,
    pub bytes_read: u64,
    pub estimated_total_rows: u64,
}

impl StatSnapshotImpl for SourceSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        stats![
            DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            ROWS_OUT_KEY; Stat::Count(self.rows_out),
            BYTES_READ_KEY; Stat::Bytes(self.bytes_read),
            ESTIMATED_TOTAL_ROWS_KEY; Stat::Count(self.estimated_total_rows),
        ]
    }

    fn to_message(&self) -> Cow<'static, str> {
        Cow::Owned(format!(", {} read", HumanBytes(self.bytes_read)))
    }

    fn current_progress(&self) -> u64 {
        self.rows_out
    }

    fn total(&self) -> u64 {
        self.estimated_total_rows
    }

    fn total_key(&self) -> &'static str {
        "rows read"
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct FilterSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub selectivity: f64,
    pub estimated_total_rows: u64,
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
            ESTIMATED_TOTAL_ROWS_KEY; Stat::Count(self.estimated_total_rows),
            "selectivity"; Stat::Percent(self.selectivity),
        ]
    }

    fn to_message(&self) -> Cow<'static, str> {
        Cow::Owned(format!(", {:.2}% after filter", self.selectivity))
    }

    fn current_progress(&self) -> u64 {
        self.rows_out
    }

    fn total(&self) -> u64 {
        self.estimated_total_rows
    }

    fn total_key(&self) -> &'static str {
        "rows out"
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ExplodeSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub amplification: f64,
    pub estimated_total_rows: u64,
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
            ESTIMATED_TOTAL_ROWS_KEY; Stat::Count(self.estimated_total_rows),
            "amplification"; Stat::Float(self.amplification),
        ]
    }

    fn to_message(&self) -> Cow<'static, str> {
        Cow::Owned(format!(", {:.2}x after explode", self.amplification))
    }

    fn current_progress(&self) -> u64 {
        self.rows_out
    }

    fn total(&self) -> u64 {
        self.estimated_total_rows
    }

    fn total_key(&self) -> &'static str {
        "rows out"
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct UdfSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub estimated_total_rows: u64,
    pub custom_counters: HashMap<Arc<str>, u64>,
}

impl StatSnapshotImpl for UdfSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries = SmallVec::with_capacity(3 + self.custom_counters.len());

        entries.push((
            DURATION_KEY.into(),
            Stat::Duration(Duration::from_micros(self.cpu_us)),
        ));
        entries.push((ROWS_IN_KEY.into(), Stat::Count(self.rows_in)));
        entries.push((ROWS_OUT_KEY.into(), Stat::Count(self.rows_out)));
        entries.push((
            ESTIMATED_TOTAL_ROWS_KEY.into(),
            Stat::Count(self.estimated_total_rows),
        ));

        for (name, value) in &self.custom_counters {
            entries.push((name.clone().into(), Stat::Count(*value)));
        }

        Stats(entries)
    }

    fn to_message(&self) -> Cow<'static, str> {
        if self.custom_counters.is_empty() {
            Cow::Borrowed("")
        } else {
            let mut custom_message = String::new();
            for (name, value) in &self.custom_counters {
                write!(custom_message, ", {} {}", HumanCount(*value), name.as_ref())
                    .expect("Failed to construct message for progress bar");
            }
            Cow::Owned(custom_message)
        }
    }

    fn current_progress(&self) -> u64 {
        self.rows_out
    }

    fn total(&self) -> u64 {
        self.estimated_total_rows
    }

    fn total_key(&self) -> &'static str {
        "rows out"
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct JoinSnapshot {
    pub duration_us: u64,
    pub build_rows_inserted: u64,
    pub probe_rows_in: u64,
    pub probe_rows_out: u64,
    pub estimated_total_probe_rows: u64,
}

impl StatSnapshotImpl for JoinSnapshot {
    fn duration_us(&self) -> u64 {
        self.duration_us
    }

    fn to_stats(&self) -> Stats {
        stats![
            DURATION_KEY; Stat::Duration(Duration::from_micros(self.duration_us)),
            JOIN_BUILD_ROWS_INSERTED_KEY; Stat::Count(self.build_rows_inserted),
            JOIN_PROBE_ROWS_IN_KEY; Stat::Count(self.probe_rows_in),
            JOIN_PROBE_ROWS_OUT_KEY; Stat::Count(self.probe_rows_out),
            ESTIMATED_TOTAL_PROBE_ROWS_KEY; Stat::Count(self.estimated_total_probe_rows),
        ]
    }

    fn to_message(&self) -> Cow<'static, str> {
        Cow::Owned(format!(
            ", {} build rows inserted",
            HumanCount(self.build_rows_inserted)
        ))
    }

    fn current_progress(&self) -> u64 {
        self.probe_rows_out
    }

    fn total(&self) -> u64 {
        self.estimated_total_probe_rows
    }

    fn total_key(&self) -> &'static str {
        "joined rows out"
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct WriteSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub estimated_total_rows: u64,
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
            ESTIMATED_TOTAL_ROWS_KEY; Stat::Count(self.estimated_total_rows),
        ]
    }

    fn to_message(&self) -> Cow<'static, str> {
        Cow::Owned(format!(", {} written", HumanBytes(self.bytes_written)))
    }

    fn current_progress(&self) -> u64 {
        self.rows_written
    }

    fn total(&self) -> u64 {
        self.estimated_total_rows
    }

    fn total_key(&self) -> &'static str {
        "rows written"
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── DefaultSnapshot ──────────────────────────────────────────────────

    #[test]
    fn default_snapshot_progress_equals_rows_out() {
        let snap = DefaultSnapshot {
            cpu_us: 0,
            rows_in: 500,
            rows_out: 300,
            estimated_total_rows: 1000,
        };
        assert_eq!(snap.current_progress(), 300);
        assert_eq!(snap.total(), 1000);
        assert_eq!(snap.total_key(), "rows out");
    }

    #[test]
    fn default_snapshot_message_is_empty() {
        let snap = DefaultSnapshot {
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            estimated_total_rows: 0,
        };
        assert_eq!(snap.to_message().as_ref(), "");
    }

    // ── SourceSnapshot ───────────────────────────────────────────────────

    #[test]
    fn source_progress_tracks_rows_out() {
        let snap = SourceSnapshot {
            cpu_us: 100,
            rows_out: 42_000,
            bytes_read: 1_000_000,
            estimated_total_rows: 100_000,
        };
        assert_eq!(snap.current_progress(), 42_000);
        assert_eq!(snap.total(), 100_000);
        assert_eq!(snap.total_key(), "rows read");
    }

    #[test]
    fn source_message_shows_bytes_read() {
        let snap = SourceSnapshot {
            cpu_us: 0,
            rows_out: 0,
            bytes_read: 1_500_000_000,
            estimated_total_rows: 0,
        };
        let msg = snap.to_message();
        assert!(
            msg.contains("read"),
            "expected 'read' in message, got: {msg}"
        );
    }

    // ── FilterSnapshot ───────────────────────────────────────────────────

    #[test]
    fn filter_total_is_child_estimate_times_selectivity() {
        // Scenario: upstream estimates 10_000 rows, filter passes 25%
        let snap = FilterSnapshot {
            cpu_us: 0,
            rows_in: 4000,
            rows_out: 1000,
            selectivity: 25.0,
            estimated_total_rows: 2500, // 10_000 * 0.25
        };
        assert_eq!(snap.current_progress(), 1000);
        assert_eq!(snap.total(), 2500);
        assert_eq!(snap.total_key(), "rows out");
    }

    #[test]
    fn filter_message_shows_selectivity_percent() {
        let snap = FilterSnapshot {
            cpu_us: 0,
            rows_in: 200,
            rows_out: 50,
            selectivity: 25.0,
            estimated_total_rows: 250,
        };
        let msg = snap.to_message();
        assert!(
            msg.contains("25.00%"),
            "expected '25.00%' in message, got: {msg}"
        );
        assert!(
            msg.contains("after filter"),
            "expected 'after filter' in message, got: {msg}"
        );
    }

    #[test]
    fn filter_100_percent_selectivity_passes_child_estimate_through() {
        // If the filter keeps everything, estimated_total == child_estimated_total
        let snap = FilterSnapshot {
            cpu_us: 0,
            rows_in: 500,
            rows_out: 500,
            selectivity: 100.0,
            estimated_total_rows: 1000, // child had 1000, selectivity 100% → 1000
        };
        assert_eq!(snap.total(), 1000);
    }

    #[test]
    fn filter_zero_selectivity_estimates_zero_rows() {
        let snap = FilterSnapshot {
            cpu_us: 0,
            rows_in: 500,
            rows_out: 0,
            selectivity: 0.0,
            estimated_total_rows: 0, // child had 1000, selectivity 0% → 0
        };
        assert_eq!(snap.total(), 0);
        assert_eq!(snap.current_progress(), 0);
    }

    // ── ExplodeSnapshot ──────────────────────────────────────────────────

    #[test]
    fn explode_total_is_child_estimate_times_amplification() {
        // Scenario: child estimates 1000 rows, each row explodes into ~3 rows
        let snap = ExplodeSnapshot {
            cpu_us: 0,
            rows_in: 200,
            rows_out: 600,
            amplification: 3.0,
            estimated_total_rows: 3000, // 1000 * 3.0
        };
        assert_eq!(snap.current_progress(), 600);
        assert_eq!(snap.total(), 3000);
        assert_eq!(snap.total_key(), "rows out");
    }

    #[test]
    fn explode_message_shows_amplification_factor() {
        let snap = ExplodeSnapshot {
            cpu_us: 0,
            rows_in: 100,
            rows_out: 250,
            amplification: 2.5,
            estimated_total_rows: 2500,
        };
        let msg = snap.to_message();
        assert!(
            msg.contains("2.50x"),
            "expected '2.50x' in message, got: {msg}"
        );
        assert!(
            msg.contains("after explode"),
            "expected 'after explode' in message, got: {msg}"
        );
    }

    #[test]
    fn explode_amplification_1x_passes_estimate_through() {
        let snap = ExplodeSnapshot {
            cpu_us: 0,
            rows_in: 500,
            rows_out: 500,
            amplification: 1.0,
            estimated_total_rows: 1000,
        };
        assert_eq!(snap.total(), 1000);
    }

    // ── UdfSnapshot ──────────────────────────────────────────────────────

    #[test]
    fn udf_passes_child_estimate_through() {
        let snap = UdfSnapshot {
            cpu_us: 0,
            rows_in: 500,
            rows_out: 500,
            estimated_total_rows: 2000, // pass-through from child
            custom_counters: HashMap::new(),
        };
        assert_eq!(snap.current_progress(), 500);
        assert_eq!(snap.total(), 2000);
        assert_eq!(snap.total_key(), "rows out");
    }

    #[test]
    fn udf_message_is_empty_without_custom_counters() {
        let snap = UdfSnapshot {
            cpu_us: 0,
            rows_in: 0,
            rows_out: 0,
            estimated_total_rows: 0,
            custom_counters: HashMap::new(),
        };
        assert_eq!(snap.to_message().as_ref(), "");
    }

    #[test]
    fn udf_message_shows_custom_counters() {
        let mut counters = HashMap::new();
        counters.insert(Arc::from("inferences"), 42);
        let snap = UdfSnapshot {
            cpu_us: 0,
            rows_in: 100,
            rows_out: 100,
            estimated_total_rows: 1000,
            custom_counters: counters,
        };
        let msg = snap.to_message();
        assert!(
            msg.contains("inferences"),
            "expected 'inferences' in message, got: {msg}"
        );
        assert!(
            msg.contains("42"),
            "expected '42' in message, got: {msg}"
        );
    }

    // ── JoinSnapshot ─────────────────────────────────────────────────────

    #[test]
    fn join_progress_tracks_probe_rows_out() {
        let snap = JoinSnapshot {
            duration_us: 0,
            build_rows_inserted: 5000,
            probe_rows_in: 10_000,
            probe_rows_out: 8000,
            estimated_total_probe_rows: 20_000,
        };
        assert_eq!(snap.current_progress(), 8000);
        assert_eq!(snap.total(), 20_000);
        assert_eq!(snap.total_key(), "joined rows out");
    }

    #[test]
    fn join_message_shows_build_rows_inserted() {
        let snap = JoinSnapshot {
            duration_us: 0,
            build_rows_inserted: 5000,
            probe_rows_in: 0,
            probe_rows_out: 0,
            estimated_total_probe_rows: 0,
        };
        let msg = snap.to_message();
        assert!(
            msg.contains("build rows inserted"),
            "expected 'build rows inserted' in message, got: {msg}"
        );
    }

    // ── WriteSnapshot ────────────────────────────────────────────────────

    #[test]
    fn write_progress_tracks_rows_written() {
        let snap = WriteSnapshot {
            cpu_us: 0,
            rows_in: 10_000,
            rows_written: 7500,
            bytes_written: 500_000,
            estimated_total_rows: 10_000,
        };
        assert_eq!(snap.current_progress(), 7500);
        assert_eq!(snap.total(), 10_000);
        assert_eq!(snap.total_key(), "rows written");
    }

    #[test]
    fn write_message_shows_bytes_written() {
        let snap = WriteSnapshot {
            cpu_us: 0,
            rows_in: 0,
            rows_written: 0,
            bytes_written: 2_000_000_000,
            estimated_total_rows: 0,
        };
        let msg = snap.to_message();
        assert!(
            msg.contains("written"),
            "expected 'written' in message, got: {msg}"
        );
    }

    // ── StatSnapshot enum dispatch ───────────────────────────────────────

    #[test]
    fn enum_dispatch_routes_to_correct_variant() {
        let filter_snap: StatSnapshot = StatSnapshot::Filter(FilterSnapshot {
            cpu_us: 100,
            rows_in: 1000,
            rows_out: 250,
            selectivity: 25.0,
            estimated_total_rows: 500,
        });
        // Verify enum_dispatch correctly delegates
        assert_eq!(filter_snap.current_progress(), 250);
        assert_eq!(filter_snap.total(), 500);
        assert_eq!(filter_snap.total_key(), "rows out");
        assert_eq!(filter_snap.duration_us(), 100);
    }
}
