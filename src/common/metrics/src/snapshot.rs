use std::{collections::HashMap, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use enum_dispatch::enum_dispatch;
use indicatif::{HumanBytes, HumanCount};
use itertools::Itertools as _;
use smallvec::SmallVec;

use crate::{
    BYTES_WRITTEN_KEY, ROWS_IN_KEY, ROWS_OUT_KEY, ROWS_WRITTEN_KEY, Stat, Stats, TASK_DURATION_KEY,
};

macro_rules! stats {
    ($($name:expr; $value:expr),* $(,)?) => {
        Stats(smallvec::smallvec![
            $( ($name.into(), $value) ),*
        ])
    };
}

#[enum_dispatch]
pub trait StatSnapshotImpl: Send + Sync {
    fn to_stats(&self) -> Stats;
    fn to_message(&self) -> String;
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct DefaultSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
}

impl StatSnapshotImpl for DefaultSnapshot {
    fn to_stats(&self) -> Stats {
        stats![
            TASK_DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct SourceSnapshot {
    pub cpu_us: u64,
    pub rows_out: u64,
    pub bytes_read: u64,
}

impl StatSnapshotImpl for SourceSnapshot {
    fn to_stats(&self) -> Stats {
        stats![
            TASK_DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            ROWS_OUT_KEY; Stat::Count(self.rows_out),
            "bytes_read"; Stat::Bytes(self.bytes_read),
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct FilterSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub selectivity: f64,
}

impl StatSnapshotImpl for FilterSnapshot {
    fn to_stats(&self) -> Stats {
        stats![
            TASK_DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct ExplodeSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub amplification: f64,
}

impl StatSnapshotImpl for ExplodeSnapshot {
    fn to_stats(&self) -> Stats {
        stats![
            TASK_DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct UdfSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_out: u64,
    pub custom_counters: HashMap<Arc<str>, u64>,
}

impl StatSnapshotImpl for UdfSnapshot {
    fn to_stats(&self) -> Stats {
        let mut entries = SmallVec::with_capacity(3 + self.custom_counters.len());

        entries.push((
            TASK_DURATION_KEY.into(),
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct HashJoinBuildSnapshot {
    pub cpu_us: u64,
    pub rows_inserted: u64,
}

impl StatSnapshotImpl for HashJoinBuildSnapshot {
    fn to_stats(&self) -> Stats {
        stats![
            TASK_DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
            "rows inserted"; Stat::Count(self.rows_inserted),
        ]
    }

    fn to_message(&self) -> String {
        format!("{} rows inserted", HumanCount(self.rows_inserted))
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct WriteSnapshot {
    pub cpu_us: u64,
    pub rows_in: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
}

impl StatSnapshotImpl for WriteSnapshot {
    fn to_stats(&self) -> Stats {
        stats![
            TASK_DURATION_KEY; Stat::Duration(Duration::from_micros(self.cpu_us)),
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
#[derive(Debug, Clone, Encode, Decode)]
pub enum StatSnapshot {
    Default(DefaultSnapshot),
    Source(SourceSnapshot),
    Filter(FilterSnapshot),
    Explode(ExplodeSnapshot),
    Udf(UdfSnapshot),
    HashJoinBuild(HashJoinBuildSnapshot),
    Write(WriteSnapshot),
}
