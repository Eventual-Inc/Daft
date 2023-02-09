use arrow2::datatypes::TimeUnit as ArrowTimeUnit;

use crate::error::DaftResult;

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum TimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl TimeUnit {
    #![allow(clippy::wrong_self_convention)]
    pub fn to_arrow(&self) -> DaftResult<ArrowTimeUnit> {
        match self {
            TimeUnit::Nanoseconds => Ok(ArrowTimeUnit::Nanosecond),
            TimeUnit::Microseconds => Ok(ArrowTimeUnit::Microsecond),
            TimeUnit::Milliseconds => Ok(ArrowTimeUnit::Millisecond),
            TimeUnit::Seconds => Ok(ArrowTimeUnit::Second),
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
