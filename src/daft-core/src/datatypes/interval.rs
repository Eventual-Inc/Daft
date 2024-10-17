use std::fmt::Display;

use arrow2::types::months_days_ns;
use common_error::DaftResult;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A literal "interval" value.
pub struct IntervalValue {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}
impl IntervalValue {
    pub fn new(months: i32, days: i32, nanos: i64) -> Self {
        Self {
            months,
            days,
            nanoseconds: nanos,
        }
    }
}

#[cfg_attr(feature = "python", pyclass)]
#[derive(Debug, Default, Clone)]
pub struct IntervalValueBuilder {
    pub years: Option<i32>,
    pub months: Option<i32>,
    pub days: Option<i32>,
    pub hours: Option<i32>,
    pub minutes: Option<i32>,
    pub seconds: Option<i32>,
    pub milliseconds: Option<i32>,
    pub nanoseconds: Option<i64>,
}

impl IntervalValueBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn years(self, years: i32) -> Self {
        Self {
            years: Some(years),
            ..self
        }
    }

    pub fn months(self, months: i32) -> Self {
        Self {
            months: Some(months),
            ..self
        }
    }

    pub fn days(self, days: i32) -> Self {
        Self {
            days: Some(days),
            ..self
        }
    }

    pub fn hours(self, hours: i32) -> Self {
        Self {
            hours: Some(hours),
            ..self
        }
    }

    pub fn minutes(self, minutes: i32) -> Self {
        Self {
            minutes: Some(minutes),
            ..self
        }
    }

    pub fn seconds(self, seconds: i32) -> Self {
        Self {
            seconds: Some(seconds),
            ..self
        }
    }

    pub fn milliseconds(self, milliseconds: i32) -> Self {
        Self {
            milliseconds: Some(milliseconds),
            ..self
        }
    }

    pub fn nanoseconds(self, nanoseconds: i64) -> Self {
        Self {
            nanoseconds: Some(nanoseconds),
            ..self
        }
    }

    pub fn build(self) -> DaftResult<IntervalValue> {
        IntervalValue::try_new(self)
    }
}

impl IntervalValue {
    const MILLIS_PER_HOUR: i32 = 3_600_000;
    const MILLIS_PER_MINUTE: i32 = 60_000;
    const MILLIS_PER_SECOND: i32 = 1_000;
    const NANOS_PER_HOUR: i64 = 3_600_000_000_000;
    const NANOS_PER_MINUTE: i64 = 60_000_000_000;
    const NANOS_PER_MILLIS: i64 = 1_000_000;
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    pub fn try_new(opts: IntervalValueBuilder) -> DaftResult<Self> {
        // Instead of always using the default values, we can skip some computations for certain patterns
        match opts {
            IntervalValueBuilder {
                years,
                months,
                days: None,
                hours: None,
                minutes: None,
                seconds: None,
                milliseconds: None,
                nanoseconds: None,
            } => {
                let years = years.unwrap_or(0);
                let months = months.unwrap_or(0);
                Ok(Self {
                    months: years * 12 + months,
                    days: 0,
                    nanoseconds: 0,
                })
            }
            IntervalValueBuilder {
                years: None,
                months: None,
                days,
                hours,
                minutes,
                seconds,
                milliseconds,
                nanoseconds: None,
            } => {
                let days = days.unwrap_or(0);
                let milliseconds = milliseconds.unwrap_or(0);
                let minutes = minutes.unwrap_or(0) * Self::MILLIS_PER_MINUTE;
                let hours = hours.unwrap_or(0) * Self::MILLIS_PER_HOUR;
                let seconds = seconds.unwrap_or(0) * Self::MILLIS_PER_SECOND;
                let nanoseconds =
                    (milliseconds + hours + minutes + seconds) as i64 * Self::NANOS_PER_MILLIS;

                Ok(Self {
                    months: 0,
                    days,
                    nanoseconds,
                })
            }
            IntervalValueBuilder {
                years: None,
                months: Some(months),
                days: Some(days),
                hours: None,
                minutes: None,
                seconds: None,
                milliseconds: None,
                nanoseconds: Some(nanoseconds),
            } => Ok(Self {
                months,
                days,
                nanoseconds,
            }),

            // Fallback with default values for all filds.
            IntervalValueBuilder {
                years,
                months,
                days,
                hours,
                minutes,
                seconds,
                milliseconds,
                nanoseconds,
            } => {
                let years = years.unwrap_or(0);
                let months = months.unwrap_or(0);
                let days = days.unwrap_or(0);
                let hours = hours.unwrap_or(0) as i64 * Self::NANOS_PER_HOUR;
                let millis = milliseconds.unwrap_or(0) as i64 * Self::NANOS_PER_MILLIS;
                let minutes = minutes.unwrap_or(0) as i64 * Self::NANOS_PER_MINUTE;
                let seconds = seconds.unwrap_or(0) as i64 * Self::NANOS_PER_SECOND;
                let nanos = nanoseconds.unwrap_or(0) + millis + hours + minutes + seconds;

                Ok(Self {
                    months: years * 12 + months,
                    days,
                    nanoseconds: nanos,
                })
            }
        }
    }
}

impl From<months_days_ns> for IntervalValue {
    fn from(value: months_days_ns) -> Self {
        Self {
            months: value.months(),
            days: value.days(),
            nanoseconds: value.ns(),
        }
    }
}

impl Display for IntervalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // from months, we convert that to years + months
        let years = self.months / 12;
        let months = self.months % 12;

        let days = self.days;

        let nanos = self.nanoseconds;
        let hours = nanos / Self::NANOS_PER_HOUR;
        let mins = (nanos % Self::NANOS_PER_HOUR) / Self::NANOS_PER_MINUTE;
        let secs = (nanos % Self::NANOS_PER_MINUTE) / Self::NANOS_PER_SECOND;
        let millis = (nanos % Self::NANOS_PER_SECOND) / Self::NANOS_PER_MILLIS;
        let remaining_nanos = nanos % Self::NANOS_PER_MILLIS;

        if years != 0 {
            write!(f, "{years}y {months}m",)?;
        } else {
            write!(f, "{months}m",)?;
        }
        write!(f, " {}d", days)?;

        if hours != 0 {
            write!(f, " {}h", hours)?;
        }

        if mins != 0 {
            write!(f, " {}m", mins)?;
        }

        if secs != 0 {
            write!(f, " {}s", secs)?;
        }

        if millis != 0 {
            write!(f, " {}ms", millis)?;
        }

        if remaining_nanos != 0 {
            write!(f, " {}ns", remaining_nanos)?;
        } else if hours == 0 && mins == 0 && secs == 0 && millis == 0 {
            write!(f, " 0ns")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use super::{IntervalValue, IntervalValueBuilder};

    #[test]
    fn test_interval_value() -> DaftResult<()> {
        let cases = vec![
            (
                IntervalValue::new(12, 0, 0),
                IntervalValueBuilder::new().years(1).build()?,
            ),
            (
                IntervalValue::new(13, 0, 0),
                IntervalValueBuilder::new().years(1).months(1).build()?,
            ),
            (
                IntervalValue::new(0, 1, 2_000_000),
                IntervalValueBuilder::new()
                    .days(1)
                    .milliseconds(2)
                    .build()?,
            ),
            (
                IntervalValue::new(0, 1, 3_600_000_000_000),
                IntervalValueBuilder::new().days(1).hours(1).build()?,
            ),
            (
                IntervalValue::new(0, 1, 3_600_001_000_000),
                IntervalValueBuilder::new()
                    .days(1)
                    .hours(1)
                    .milliseconds(1)
                    .build()?,
            ),
            (
                IntervalValue::new(1, 2, 3),
                IntervalValueBuilder::new()
                    .months(1)
                    .days(2)
                    .nanoseconds(3)
                    .build()?,
            ),
        ];

        for (expected, actual) in cases {
            assert_eq!(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_display_for_interval() {
        let iv = IntervalValue::new(1, 2, 3);
        assert_eq!("1m 2d 3ns", iv.to_string());

        let iv = IntervalValue::new(0, 0, 0);
        assert_eq!("0m 0d 0ns", iv.to_string());

        let iv = IntervalValue::new(74, 22, 1_000_000);
        assert_eq!("6y 2m 22d 1ms", iv.to_string());

        let iv = IntervalValue::new(0, 0, 1_000_000);
        assert_eq!("0m 0d 1ms", iv.to_string());

        let iv = IntervalValue::new(0, 0, 1);
        assert_eq!("0m 0d 1ns", iv.to_string());

        let iv = IntervalValue::new(0, 0, 3_661_000_000_123);
        assert_eq!("0m 0d 1h 1m 1s 123ns", iv.to_string());

        let iv = IntervalValue::new(32, 28, 7_384_567_890_123);
        assert_eq!("2y 8m 28d 2h 3m 4s 567ms 890123ns", iv.to_string());
    }
}
