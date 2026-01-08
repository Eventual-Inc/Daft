use std::fmt::Write;

use common_display::table_display::StrValue;
use itertools::Itertools;

use crate::{datatypes::TimeUnit, series::Series};

pub fn display_date32(val: i32) -> String {
    let epoch_date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date = if val.is_positive() {
        epoch_date + chrono::naive::Days::new(val as u64)
    } else {
        epoch_date - chrono::naive::Days::new(val.unsigned_abs() as u64)
    };
    format!("{date}")
}

pub fn display_time64(val: i64, unit: &TimeUnit) -> String {
    let time = match unit {
        TimeUnit::Nanoseconds => Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(
            (val / 1_000_000_000) as u32,
            (val % 1_000_000_000) as u32,
        )
        .unwrap()),
        TimeUnit::Microseconds => Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(
            (val / 1_000_000) as u32,
            ((val % 1_000_000) * 1_000) as u32,
        )
        .unwrap()),
        TimeUnit::Milliseconds => {
            let seconds = u32::try_from(val / 1_000);
            let nanoseconds = u32::try_from((val % 1_000) * 1_000_000);
            match (seconds, nanoseconds) {
                (Ok(secs), Ok(nano)) => {
                    Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).unwrap())
                }
                (Err(e), _) => Err(e),
                (_, Err(e)) => Err(e),
            }
        }
        TimeUnit::Seconds => {
            let seconds = u32::try_from(val);
            match seconds {
                Ok(secs) => {
                    Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, 0).unwrap())
                }
                Err(e) => Err(e),
            }
        }
    };

    match time {
        Ok(time) => format!("{time}"),
        Err(e) => format!("Display Error: {e}"),
    }
}

pub fn display_timestamp(val: i64, unit: &TimeUnit, timezone: &Option<String>) -> String {
    use crate::array::ops::cast::{
        timestamp_to_str_naive, timestamp_to_str_offset, timestamp_to_str_tz,
    };

    timezone.as_ref().map_or_else(
        || timestamp_to_str_naive(val, unit),
        |timezone| {
            // In arrow, timezone string can be either:
            // 1. a fixed offset "-07:00", parsed using parse_offset, or
            // 2. a timezone name e.g. "America/Los_Angeles", parsed using parse_offset_tz.
            if let Ok(offset) = daft_schema::time_unit::parse_offset(timezone) {
                timestamp_to_str_offset(val, unit, &offset)
            } else if let Ok(tz) = daft_schema::time_unit::parse_offset_tz(timezone) {
                timestamp_to_str_tz(val, unit, &tz)
            } else {
                panic!("Unable to parse timezone string {}", timezone)
            }
        },
    )
}

const UNITS: [&str; 4] = ["d", "h", "m", "s"];
const SIZES: [[i64; 4]; 4] = [
    [
        86_400_000_000_000,
        3_600_000_000_000,
        60_000_000_000,
        1_000_000_000,
    ], // Nanoseconds
    [86_400_000_000, 3_600_000_000, 60_000_000, 1_000_000], // Microseconds
    [86_400_000, 3_600_000, 60_000, 1_000],                 // Milliseconds
    [86_400, 3_600, 60, 1],                                 // Seconds
];

pub fn display_duration(val: i64, unit: &TimeUnit) -> String {
    let mut output = String::new();
    let (sizes, suffix, remainder_divisor) = match unit {
        TimeUnit::Nanoseconds => (&SIZES[0], "ns", 1_000_000_000),
        TimeUnit::Microseconds => (&SIZES[1], "µs", 1_000_000),
        TimeUnit::Milliseconds => (&SIZES[2], "ms", 1_000),
        TimeUnit::Seconds => (&SIZES[3], "s", 1),
    };

    if val == 0 {
        return format!("0{}", suffix);
    }

    for (i, &size) in sizes.iter().enumerate() {
        let whole_num = if i == 0 {
            val / size
        } else {
            (val % sizes[i - 1]) / size
        };
        if whole_num != 0 {
            write!(output, "{}{}", whole_num, UNITS[i]).unwrap();
            if val % size != 0 {
                output.push(' ');
            }
        }
    }

    let remainder = val % remainder_divisor;
    if remainder != 0 && suffix != "s" {
        write!(output, "{}{}", remainder, suffix).unwrap();
    }

    output
}

pub fn display_decimal128(val: i128, _precision: u8, scale: i8) -> String {
    if scale < 0 {
        unimplemented!();
    } else {
        let modulus = i128::pow(10, scale as u32);
        let integral = val / modulus;
        if scale == 0 {
            format!("{}", integral)
        } else {
            let sign = if val < 0 { "-" } else { "" };
            let integral = integral.abs();
            let decimals = (val % modulus).abs();
            let scale = scale as usize;
            format!("{}{}.{:0scale$}", sign, integral, decimals)
        }
    }
}

#[must_use]
pub fn display_series_in_literal(series: &Series) -> String {
    if !series.is_empty() {
        format!(
            "[{}]",
            (0..series.len()).map(|i| series.str_value(i)).join(", ")
        )
    } else {
        "[]".to_string()
    }
}
