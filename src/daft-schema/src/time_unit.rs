use std::str::FromStr;

use arrow2::datatypes::TimeUnit as ArrowTimeUnit;
use common_error::DaftError;
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
    #[must_use]
    pub fn to_arrow(&self) -> ArrowTimeUnit {
        match self {
            Self::Nanoseconds => ArrowTimeUnit::Nanosecond,
            Self::Microseconds => ArrowTimeUnit::Microsecond,
            Self::Milliseconds => ArrowTimeUnit::Millisecond,
            Self::Seconds => ArrowTimeUnit::Second,
        }
    }

    #[must_use]
    pub fn to_scale_factor(&self) -> i64 {
        match self {
            Self::Seconds => 1,
            Self::Milliseconds => 1000,
            Self::Microseconds => 1_000_000,
            Self::Nanoseconds => 1_000_000_000,
        }
    }
}

impl FromStr for TimeUnit {
    type Err = DaftError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ns" | "nanoseconds" => Ok(Self::Nanoseconds),
            "us" | "microseconds" => Ok(Self::Microseconds),
            "ms" | "milliseconds" => Ok(Self::Milliseconds),
            "s" | "seconds" => Ok(Self::Seconds),
            _ => Err(DaftError::ValueError("Invalid time unit".to_string())),
        }
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(tu: &ArrowTimeUnit) -> Self {
        match tu {
            ArrowTimeUnit::Nanosecond => Self::Nanoseconds,
            ArrowTimeUnit::Microsecond => Self::Microseconds,
            ArrowTimeUnit::Millisecond => Self::Milliseconds,
            ArrowTimeUnit::Second => Self::Seconds,
        }
    }
}

#[must_use]
pub fn infer_timeunit_from_format_string(format: &str) -> TimeUnit {
    if format.contains("%9f") || format.contains("%.9f") {
        TimeUnit::Nanoseconds
    } else if format.contains("%3f") || format.contains("%.3f") {
        TimeUnit::Milliseconds
    } else {
        TimeUnit::Microseconds
    }
}

#[must_use]
pub fn format_string_has_offset(format: &str) -> bool {
    // These are all valid chrono formats that contain an offset
    format.contains("%Z")
        || format.contains("%z")
        || format.contains("%:z")
        || format.contains("%::z")
        || format.contains("%#z")
        || format == "%+"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_string_has_offset() {
        let test_cases = vec![
            ("%Y-%m-%d %H:%M:%S %Z", true),
            ("%Y-%m-%d %H:%M:%S %z", true),
            ("%Y-%m-%d %H:%M:%S %:z", true),
            ("%Y-%m-%d %H:%M:%S %::z", true),
            ("%Y-%m-%d %H:%M:%S %#z", true),
            ("%+", true),
            ("Date: %Y-%m-%d Time: %H:%M:%S Zone: %Z", true),
            ("%Y-%m-%dT%H:%M:%S%z", true),
            ("%Y-%m-%d %H:%M:%S.%f %z", true),
            ("%c %Z", true),
            ("%Y-%m-%d %H:%M:%S", false),
            ("%Y-%m-%dT%H:%M:%S", false),
            ("%Y z %d", false),
            ("%Y-%m-%d", false),
            ("%H:%M:%S", false),
            ("%Y-%m-%d %H:%M:%S.%f", false),
            ("", false),
            ("random text", false),
            ("%%z", true),
        ];

        for (format, expected) in test_cases {
            assert_eq!(
                format_string_has_offset(format),
                expected,
                "Failed for format: {}",
                format
            );
        }
    }
}
