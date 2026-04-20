use std::{collections::HashMap, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use enum_dispatch::enum_dispatch;
use indicatif::{HumanBytes, HumanCount};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    BYTES_IN_KEY, BYTES_OUT_KEY, BYTES_READ_KEY, BYTES_WRITTEN_KEY, DURATION_KEY,
    JOIN_BUILD_BYTES_INSERTED_KEY, JOIN_PROBE_BYTES_IN_KEY, JOIN_PROBE_BYTES_OUT_KEY, ROWS_IN_KEY,
    ROWS_OUT_KEY, ROWS_WRITTEN_KEY, Stat, Stats,
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
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct DefaultSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    #[serde(default)]
    pub bytes_in: u64,
    #[serde(default)]
    pub bytes_out: u64,
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
            BYTES_IN_KEY; Stat::Bytes(self.bytes_in),
            BYTES_OUT_KEY; Stat::Bytes(self.bytes_out),
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows out",
            HumanCount(self.rows_in),
            HumanCount(self.rows_out),
        )
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
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SourceSnapshot {
    pub cpu_us: u64,
    pub rows_out: u64,
    pub bytes_read: u64,
    #[serde(default)]
    pub bytes_out: u64,
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
            BYTES_OUT_KEY; Stat::Bytes(self.bytes_out),
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows out, {} read",
            HumanCount(self.rows_out),
            HumanBytes(self.bytes_read),
        )
    }
}

impl SourceSnapshot {
    pub fn merge(self, other: &Self) -> Self {
        Self {
            cpu_us: self.cpu_us + other.cpu_us,
            rows_out: self.rows_out + other.rows_out,
            bytes_read: self.bytes_read + other.bytes_read,
            bytes_out: self.bytes_out + other.bytes_out,
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
}

impl StatSnapshotImpl for UdfSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries = SmallVec::with_capacity(5 + self.custom_counters.len());

        entries.push((
            DURATION_KEY.into(),
            Stat::Duration(Duration::from_micros(self.cpu_us)),
        ));
        entries.push((ROWS_IN_KEY.into(), Stat::Count(self.rows_in)));
        entries.push((ROWS_OUT_KEY.into(), Stat::Count(self.rows_out)));
        entries.push((BYTES_IN_KEY.into(), Stat::Bytes(self.bytes_in)));
        entries.push((BYTES_OUT_KEY.into(), Stat::Bytes(self.bytes_out)));

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
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
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
}

impl StatSnapshotImpl for JoinSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        stats![
            DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            "build rows inserted"; Stat::Count(self.build_rows_inserted),
            "probe rows in"; Stat::Count(self.probe_rows_in),
            "probe rows out"; Stat::Count(self.probe_rows_out),
            JOIN_BUILD_BYTES_INSERTED_KEY; Stat::Bytes(self.build_bytes_inserted),
            JOIN_PROBE_BYTES_IN_KEY; Stat::Bytes(self.probe_bytes_in),
            JOIN_PROBE_BYTES_OUT_KEY; Stat::Bytes(self.probe_bytes_out),
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} build rows inserted, {} probe rows in, {} probe rows out",
            HumanCount(self.build_rows_inserted),
            HumanCount(self.probe_rows_in),
            HumanCount(self.probe_rows_out),
        )
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
