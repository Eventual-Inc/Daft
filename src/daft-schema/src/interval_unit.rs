use arrow2::datatypes::IntervalUnit as ArrowIntervalUnit;
use derive_more::derive::Display;
use serde::{Deserialize, Serialize};

#[derive(
    Copy, Clone, Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
/// Interval units defined in Daft
pub enum IntervalUnit {
    /// The number of elapsed whole months.
    YearMonth,
    /// The number of elapsed days and milliseconds,
    /// stored as 2 contiguous `i32`
    DayTime,
    /// The number of elapsed months (i32), days (i32) and nanoseconds (i64).
    MonthDayNano,
}

impl IntervalUnit {
    #![allow(clippy::wrong_self_convention)]
    pub fn to_arrow(&self) -> ArrowIntervalUnit {
        match self {
            Self::YearMonth => ArrowIntervalUnit::YearMonth,
            Self::DayTime => ArrowIntervalUnit::DayTime,
            Self::MonthDayNano => ArrowIntervalUnit::MonthDayNano,
        }
    }
}
