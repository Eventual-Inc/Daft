use parquet2::{schema::types::TimeUnit, types::int96_to_i64_ns};

pub fn convert_i128(value: &[u8], n: usize) -> i128 {
    // Copy the fixed-size byte value to the start of a 16 byte stack
    // allocated buffer, then use an arithmetic right shift to fill in
    // MSBs, which accounts for leading 1's in negative (two's complement)
    // values.
    let mut bytes = [0u8; 16];
    bytes[..n].copy_from_slice(value);
    i128::from_be_bytes(bytes) >> (8 * (16 - n))
}
#[inline]
fn int96_to_i64_us(value: [u32; 3]) -> i64 {
    const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
    const SECONDS_PER_DAY: i64 = 86_400;
    const MICROS_PER_SECOND: i64 = 1_000_000;

    let day = i64::from(value[2]);
    let microseconds = ((i64::from(value[1]) << 32) + i64::from(value[0])) / 1_000;
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;

    seconds * MICROS_PER_SECOND + microseconds
}

#[inline]
fn int96_to_i64_ms(value: [u32; 3]) -> i64 {
    const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
    const SECONDS_PER_DAY: i64 = 86_400;
    const MILLIS_PER_SECOND: i64 = 1_000;

    let day = i64::from(value[2]);
    let milliseconds = ((i64::from(value[1]) << 32) + i64::from(value[0])) / 1_000_000;
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;

    seconds * MILLIS_PER_SECOND + milliseconds
}

#[inline]
pub fn convert_i96_to_i64_timestamp(value: [u32; 3], tu: TimeUnit) -> i64 {
    match tu {
        TimeUnit::Nanoseconds => int96_to_i64_ns(value),
        TimeUnit::Microseconds => int96_to_i64_us(value),
        TimeUnit::Milliseconds => int96_to_i64_ms(value),
    }
}

/// Standalone INT96 to nanosecond timestamp conversion (no parquet2 dependency).
#[inline]
fn int96_to_i64_ns_standalone(value: [u32; 3]) -> i64 {
    const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
    const SECONDS_PER_DAY: i64 = 86_400;
    const NANOS_PER_SECOND: i64 = 1_000_000_000;

    let day = i64::from(value[2]);
    let nanoseconds = (i64::from(value[1]) << 32) + i64::from(value[0]);
    let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;

    seconds * NANOS_PER_SECOND + nanoseconds
}

/// Convert INT96 value to i64 timestamp using Daft's TimeUnit (parquet2-agnostic).
#[inline]
pub fn convert_i96_to_i64_timestamp_daft(
    value: [u32; 3],
    tu: daft_core::datatypes::TimeUnit,
) -> i64 {
    match tu {
        daft_core::datatypes::TimeUnit::Nanoseconds => int96_to_i64_ns_standalone(value),
        daft_core::datatypes::TimeUnit::Microseconds => int96_to_i64_us(value),
        daft_core::datatypes::TimeUnit::Milliseconds => int96_to_i64_ms(value),
        daft_core::datatypes::TimeUnit::Seconds => {
            int96_to_i64_ns_standalone(value) / 1_000_000_000
        }
    }
}

/// Convert arrow-rs parquet TimeUnit to Daft TimeUnit.
pub fn arrowrs_timeunit_to_daft(unit: parquet::basic::TimeUnit) -> daft_core::datatypes::TimeUnit {
    match unit {
        parquet::basic::TimeUnit::MILLIS => daft_core::datatypes::TimeUnit::Milliseconds,
        parquet::basic::TimeUnit::MICROS => daft_core::datatypes::TimeUnit::Microseconds,
        parquet::basic::TimeUnit::NANOS => daft_core::datatypes::TimeUnit::Nanoseconds,
    }
}
