use arrow2::datatypes::TimeUnit as ArrowTimeUnit;
use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(
    Copy, Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub enum TimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl TimeUnit {
    #![allow(clippy::wrong_self_convention)]
    pub fn to_arrow(&self) -> ArrowTimeUnit {
        match self {
            TimeUnit::Nanoseconds => ArrowTimeUnit::Nanosecond,
            TimeUnit::Microseconds => ArrowTimeUnit::Microsecond,
            TimeUnit::Milliseconds => ArrowTimeUnit::Millisecond,
            TimeUnit::Seconds => ArrowTimeUnit::Second,
        }
    }

    pub fn to_scale_factor(&self) -> i64 {
        match self {
            TimeUnit::Seconds => 1,
            TimeUnit::Milliseconds => 1000,
            TimeUnit::Microseconds => 1_000_000,
            TimeUnit::Nanoseconds => 1_000_000_000,
        }
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(tu: &ArrowTimeUnit) -> Self {
        match tu {
            ArrowTimeUnit::Nanosecond => TimeUnit::Nanoseconds,
            ArrowTimeUnit::Microsecond => TimeUnit::Microseconds,
            ArrowTimeUnit::Millisecond => TimeUnit::Milliseconds,
            ArrowTimeUnit::Second => TimeUnit::Seconds,
        }
    }
}

pub fn infer_timeunit_from_format_string(format: &str) -> TimeUnit {
    if format.contains("%9f") || format.contains("%.9f") {
        TimeUnit::Nanoseconds
    } else if format.contains("%3f") || format.contains("%.3f") {
        TimeUnit::Milliseconds
    } else {
        TimeUnit::Microseconds
    }
}
