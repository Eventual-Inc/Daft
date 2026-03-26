use std::{collections::HashMap, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use enum_dispatch::enum_dispatch;
use indicatif::{HumanBytes, HumanCount};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    BYTES_READ_KEY, BYTES_WRITTEN_KEY, DURATION_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, ROWS_WRITTEN_KEY,
    Stat, Stats,
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
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows out",
            HumanCount(self.rows_in),
            HumanCount(self.rows_out)
        )
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct SourceSnapshot {
    pub cpu_us: u64,
    pub rows_out: u64,
    pub bytes_read: u64,
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
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows out, {} read",
            HumanCount(self.rows_out),
            HumanBytes(self.bytes_read)
        )
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct FilterSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub selectivity: f64,
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
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows out, {:.2}% kept",
            HumanCount(self.rows_in),
            HumanCount(self.rows_out),
            self.selectivity
        )
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ExplodeSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub amplification: f64,
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
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows out, {:.2}x inc",
            HumanCount(self.rows_in),
            HumanCount(self.rows_out),
            self.amplification
        )
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct UdfSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
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
                HumanCount(self.rows_out)
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

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct JoinSnapshot {
    pub cpu_us: u64,
    pub build_rows_inserted: u64,
    pub probe_rows_in: u64,
    pub probe_rows_out: u64,
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
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} build rows inserted, {} probe rows in, {} probe rows out",
            HumanCount(self.build_rows_inserted),
            HumanCount(self.probe_rows_in),
            HumanCount(self.probe_rows_out)
        )
    }
}

#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct WriteSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
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
        ]
    }

    fn to_message(&self) -> String {
        format!(
            "{} rows in, {} rows written, {} written",
            HumanCount(self.rows_in),
            HumanCount(self.rows_written),
            HumanBytes(self.bytes_written)
        )
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
            (Self::Default(a), Self::Default(b)) => Self::Default(DefaultSnapshot {
                cpu_us: a.cpu_us + b.cpu_us,
                rows_in: a.rows_in + b.rows_in,
                rows_out: a.rows_out + b.rows_out,
            }),
            (Self::Source(a), Self::Source(b)) => Self::Source(SourceSnapshot {
                cpu_us: a.cpu_us + b.cpu_us,
                rows_out: a.rows_out + b.rows_out,
                bytes_read: a.bytes_read + b.bytes_read,
            }),
            (Self::Filter(a), Self::Filter(b)) => {
                let rows_in = a.rows_in + b.rows_in;
                let rows_out = a.rows_out + b.rows_out;
                let selectivity = if rows_in > 0 {
                    (rows_out as f64 / rows_in as f64) * 100.0
                } else {
                    0.0
                };
                Self::Filter(FilterSnapshot {
                    cpu_us: a.cpu_us + b.cpu_us,
                    rows_in,
                    rows_out,
                    selectivity,
                })
            }
            (Self::Explode(a), Self::Explode(b)) => {
                let rows_in = a.rows_in + b.rows_in;
                let rows_out = a.rows_out + b.rows_out;
                let amplification = if rows_in > 0 {
                    rows_out as f64 / rows_in as f64
                } else {
                    0.0
                };
                Self::Explode(ExplodeSnapshot {
                    cpu_us: a.cpu_us + b.cpu_us,
                    rows_in,
                    rows_out,
                    amplification,
                })
            }
            (Self::Udf(a), Self::Udf(b)) => {
                let mut custom_counters = a.custom_counters;
                for (k, v) in &b.custom_counters {
                    *custom_counters.entry(k.clone()).or_insert(0) += v;
                }
                Self::Udf(UdfSnapshot {
                    cpu_us: a.cpu_us + b.cpu_us,
                    rows_in: a.rows_in + b.rows_in,
                    rows_out: a.rows_out + b.rows_out,
                    custom_counters,
                })
            }
            (Self::Join(a), Self::Join(b)) => Self::Join(JoinSnapshot {
                cpu_us: a.cpu_us + b.cpu_us,
                build_rows_inserted: a.build_rows_inserted + b.build_rows_inserted,
                probe_rows_in: a.probe_rows_in + b.probe_rows_in,
                probe_rows_out: a.probe_rows_out + b.probe_rows_out,
            }),
            (Self::Write(a), Self::Write(b)) => Self::Write(WriteSnapshot {
                cpu_us: a.cpu_us + b.cpu_us,
                rows_in: a.rows_in + b.rows_in,
                rows_written: a.rows_written + b.rows_written,
                bytes_written: a.bytes_written + b.bytes_written,
            }),
            (s, _) => s,
        }
    }
}
