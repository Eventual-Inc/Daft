use std::{collections::HashMap, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use enum_dispatch::enum_dispatch;
use indicatif::{HumanBytes, HumanCount};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    BYTES_READ_KEY, BYTES_RETAINED_KEY, BYTES_RETAINED_PEAK_KEY, BYTES_WRITTEN_KEY, DURATION_KEY,
    ROWS_IN_KEY, ROWS_OUT_KEY, ROWS_WRITTEN_KEY, Stat, Stats,
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
    pub bytes_retained: u64,
    pub peak_bytes_retained: u64,
}

impl StatSnapshotImpl for DefaultSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries = SmallVec::with_capacity(5);
        entries.push((
            DURATION_KEY.into(),
            Stat::Duration(Duration::from_micros(self.cpu_us)),
        ));
        entries.push((ROWS_IN_KEY.into(), Stat::Count(self.rows_in)));
        entries.push((ROWS_OUT_KEY.into(), Stat::Count(self.rows_out)));
        if self.bytes_retained > 0 {
            entries.push((BYTES_RETAINED_KEY.into(), Stat::Bytes(self.bytes_retained)));
        }
        if self.peak_bytes_retained > 0 {
            entries.push((
                BYTES_RETAINED_PEAK_KEY.into(),
                Stat::Bytes(self.peak_bytes_retained),
            ));
        }
        Stats(entries)
    }

    fn to_message(&self) -> String {
        if self.bytes_retained > 0 {
            format!(
                "{} rows in, {} rows out, {} retained",
                HumanCount(self.rows_in),
                HumanCount(self.rows_out),
                HumanBytes(self.bytes_retained)
            )
        } else if self.peak_bytes_retained > 0 {
            format!(
                "{} rows in, {} rows out, peak {} retained",
                HumanCount(self.rows_in),
                HumanCount(self.rows_out),
                HumanBytes(self.peak_bytes_retained)
            )
        } else {
            format!(
                "{} rows in, {} rows out",
                HumanCount(self.rows_in),
                HumanCount(self.rows_out)
            )
        }
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
    pub bytes_retained: u64,
    pub peak_bytes_retained: u64,
}

impl StatSnapshotImpl for JoinSnapshot {
    fn duration_us(&self) -> u64 {
        self.cpu_us
    }

    fn to_stats(&self) -> Stats {
        let mut entries = SmallVec::with_capacity(6);
        entries.push((
            DURATION_KEY.into(),
            Stat::Duration(Duration::from_micros(self.cpu_us)),
        ));
        entries.push((
            "build rows inserted".into(),
            Stat::Count(self.build_rows_inserted),
        ));
        entries.push(("probe rows in".into(), Stat::Count(self.probe_rows_in)));
        entries.push(("probe rows out".into(), Stat::Count(self.probe_rows_out)));
        if self.bytes_retained > 0 {
            entries.push((BYTES_RETAINED_KEY.into(), Stat::Bytes(self.bytes_retained)));
        }
        if self.peak_bytes_retained > 0 {
            entries.push((
                BYTES_RETAINED_PEAK_KEY.into(),
                Stat::Bytes(self.peak_bytes_retained),
            ));
        }
        Stats(entries)
    }

    fn to_message(&self) -> String {
        let base = format!(
            "{} build rows inserted, {} probe rows in, {} probe rows out",
            HumanCount(self.build_rows_inserted),
            HumanCount(self.probe_rows_in),
            HumanCount(self.probe_rows_out)
        );
        if self.bytes_retained > 0 {
            format!("{}, {} retained", base, HumanBytes(self.bytes_retained))
        } else if self.peak_bytes_retained > 0 {
            format!(
                "{}, peak {} retained",
                base,
                HumanBytes(self.peak_bytes_retained)
            )
        } else {
            base
        }
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
