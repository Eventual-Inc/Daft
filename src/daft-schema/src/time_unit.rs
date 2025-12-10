use std::{fmt::Display, str::FromStr};

use arrow_schema::TimeUnit as ArrowTimeUnit;
use common_error::DaftError;
use daft_arrow::datatypes::TimeUnit as Arrow2TimeUnit;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum TimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nanoseconds => write!(f, "ns"),
            Self::Microseconds => write!(f, "us"),
            Self::Milliseconds => write!(f, "ms"),
            Self::Seconds => write!(f, "s"),
        }
    }
}

impl TimeUnit {
    #![allow(clippy::wrong_self_convention)]
    #[must_use]
    #[deprecated(note = "use .to_arrow")]
    pub fn to_arrow2(&self) -> Arrow2TimeUnit {
        match self {
            Self::Nanoseconds => Arrow2TimeUnit::Nanosecond,
            Self::Microseconds => Arrow2TimeUnit::Microsecond,
            Self::Milliseconds => Arrow2TimeUnit::Millisecond,
            Self::Seconds => Arrow2TimeUnit::Second,
        }
    }
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

impl From<&Arrow2TimeUnit> for TimeUnit {
    fn from(tu: &Arrow2TimeUnit) -> Self {
        match tu {
            Arrow2TimeUnit::Nanosecond => Self::Nanoseconds,
            Arrow2TimeUnit::Microsecond => Self::Microseconds,
            Arrow2TimeUnit::Millisecond => Self::Milliseconds,
            Arrow2TimeUnit::Second => Self::Seconds,
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
