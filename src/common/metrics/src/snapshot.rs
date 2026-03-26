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
        self.rows_in
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
