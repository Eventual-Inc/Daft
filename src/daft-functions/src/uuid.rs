use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use chrono::{DateTime, Datelike};
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, FixedSizeBinaryArray, FromArrow, Int64Array, Schema, UuidArray},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{
        FunctionArgs, ScalarUDF, UnaryArg,
        scalar::{EvalContext, ScalarFn},
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid as RustUuid;

/// Number of bytes in a UUID (128 bits).
const UUID_LEN: usize = 16;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Uuid;

#[typetag::serde]
impl ScalarUDF for Uuid {
    fn name(&self) -> &'static str {
        "uuid"
    }

    fn call(&self, _inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let len = ctx.row_count;

        let mut builder = FixedSizeBinaryBuilder::with_capacity(len, UUID_LEN as i32);
        for _ in 0..len {
            builder.append_value(RustUuid::new_v4())?;
        }
        uuid_series_from_builder(builder)
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        if !inputs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Expected 0 input args, got {}",
                inputs.len()
            )));
        }
        Ok(Field::new("", DataType::Uuid))
    }

    fn docstring(&self) -> &'static str {
        "Generates a column of UUIDv4 values."
    }
}

#[must_use]
pub fn uuid() -> ExprRef {
    ScalarFn::builtin(Uuid, vec![]).into()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UuidV7;

#[typetag::serde]
impl ScalarUDF for UuidV7 {
    fn name(&self) -> &'static str {
        "uuidv7"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["uuid_v7"]
    }

    fn call(&self, _inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let array = uuid_v7_kernel(ctx.row_count)?;
        uuid_series_from_builder(array)
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        if !inputs.is_empty() {
            return Err(DaftError::ValueError(format!(
                "Expected 0 input args, got {}",
                inputs.len()
            )));
        }
        Ok(Field::new("", DataType::Uuid))
    }

    fn docstring(&self) -> &'static str {
        "Generates a column of UUIDv7 values."
    }
}

#[must_use]
pub fn uuidv7() -> ExprRef {
    ScalarFn::builtin(UuidV7, vec![]).into()
}

// ---------------------------------------------------------------------------
// UUIDv7 timestamp-extraction partition transforms.
//
// A UUIDv7 packs a 48-bit big-endian Unix-millisecond timestamp in its first 6
// bytes (RFC 9562 §5.7). These functions decode that timestamp and bucket it,
// mirroring the Iceberg-style partition transforms (`partition_hours`, etc.):
//   - minute / hour / day: count of units since the Unix epoch (floor division)
//   - month: calendar months since 1970-01 (== (year-1970)*12 + (month-1)),
//            matching `partition_months`.
// The input is a UUID (`DataType::Uuid`) or a `FixedSizeBinary(16)`; the output
// is `Int64`. The UUID *version*/*variant* bits do not affect the result — only
// the leading 48 timestamp bits are read — but inputs are expected to be v7.
// ---------------------------------------------------------------------------

/// Which UUIDv7 timestamp bucket to extract.
#[derive(Clone, Copy)]
enum Uuid7Unit {
    Minute,
    Hour,
    Day,
    Month,
}

/// Reads the 48-bit big-endian Unix-millisecond timestamp from the first 6 bytes
/// of a UUIDv7.
#[inline]
fn ms_from_uuid7(bytes: &[u8; UUID_LEN]) -> i64 {
    (i64::from(bytes[0]) << 40)
        | (i64::from(bytes[1]) << 32)
        | (i64::from(bytes[2]) << 24)
        | (i64::from(bytes[3]) << 16)
        | (i64::from(bytes[4]) << 8)
        | i64::from(bytes[5])
}

/// Buckets a UUIDv7 millisecond timestamp into the requested unit.
#[inline]
fn bucket_uuid7(bytes: &[u8; UUID_LEN], unit: Uuid7Unit) -> i64 {
    let ms = ms_from_uuid7(bytes);
    match unit {
        Uuid7Unit::Minute => ms.div_euclid(60_000),
        Uuid7Unit::Hour => ms.div_euclid(3_600_000),
        Uuid7Unit::Day => ms.div_euclid(86_400_000),
        // A 48-bit (always non-negative) millisecond timestamp is always within
        // chrono's representable range, so this never fails for a real UUIDv7.
        Uuid7Unit::Month => {
            let dt = DateTime::from_timestamp_millis(ms)
                .expect("48-bit millisecond timestamp is always representable");
            (i64::from(dt.year()) - 1970) * 12 + (i64::from(dt.month()) - 1)
        }
    }
}

/// Returns the input's bytes as a `FixedSizeBinaryArray`, accepting either a
/// `DataType::Uuid` (read through its physical array) or a `FixedSizeBinary(16)`.
fn uuid7_fixed_size_binary<'a>(
    input: &'a Series,
    fn_name: &str,
) -> DaftResult<&'a FixedSizeBinaryArray> {
    match input.data_type() {
        DataType::Uuid => Ok(&input.downcast::<UuidArray>()?.physical),
        DataType::FixedSizeBinary(UUID_LEN) => input.downcast::<FixedSizeBinaryArray>(),
        other => Err(DaftError::TypeError(format!(
            "Expected input to '{fn_name}' to be a Uuid or FixedSizeBinary(16), got {other}"
        ))),
    }
}

fn extract_uuid7(input: &Series, unit: Uuid7Unit, fn_name: &str) -> DaftResult<Series> {
    let bytes = uuid7_fixed_size_binary(input, fn_name)?;
    let field = Field::new(input.name(), DataType::Int64);
    let result = Int64Array::from_iter(
        field,
        bytes.into_iter().map(|b| {
            b.map(|b| {
                // `uuid7_fixed_size_binary` guarantees each value is exactly 16 bytes wide.
                let bytes: &[u8; UUID_LEN] = b
                    .try_into()
                    .expect("FixedSizeBinary(16) values are always 16 bytes");
                bucket_uuid7(bytes, unit)
            })
        }),
    );
    Ok(result.into_series())
}

/// Validates the input dtype and returns the `Int64` output field. Shared by all
/// four extraction functions' `get_return_field`.
fn uuid7_return_field(
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
    fn_name: &str,
) -> DaftResult<Field> {
    let UnaryArg { input } = inputs.try_into()?;
    let field = input.to_field(schema)?;
    match field.dtype {
        DataType::Uuid | DataType::FixedSizeBinary(UUID_LEN) => {
            Ok(Field::new(field.name, DataType::Int64))
        }
        other => Err(DaftError::TypeError(format!(
            "Expected input to '{fn_name}' to be a Uuid or FixedSizeBinary(16), got {other}"
        ))),
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ExtractMinuteUuid7;

#[typetag::serde]
impl ScalarUDF for ExtractMinuteUuid7 {
    fn name(&self) -> &'static str {
        "extract_minute_uuid7"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        extract_uuid7(&input, Uuid7Unit::Minute, self.name())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        uuid7_return_field(inputs, schema, self.name())
    }

    fn docstring(&self) -> &'static str {
        "Extracts the number of minutes since the Unix epoch from the timestamp embedded in a UUIDv7."
    }
}

#[must_use]
pub fn extract_minute_uuid7(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(ExtractMinuteUuid7, vec![input]).into()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ExtractHourUuid7;

#[typetag::serde]
impl ScalarUDF for ExtractHourUuid7 {
    fn name(&self) -> &'static str {
        "extract_hour_uuid7"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        extract_uuid7(&input, Uuid7Unit::Hour, self.name())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        uuid7_return_field(inputs, schema, self.name())
    }

    fn docstring(&self) -> &'static str {
        "Extracts the number of hours since the Unix epoch from the timestamp embedded in a UUIDv7."
    }
}

#[must_use]
pub fn extract_hour_uuid7(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(ExtractHourUuid7, vec![input]).into()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ExtractDayUuid7;

#[typetag::serde]
impl ScalarUDF for ExtractDayUuid7 {
    fn name(&self) -> &'static str {
        "extract_day_uuid7"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        extract_uuid7(&input, Uuid7Unit::Day, self.name())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        uuid7_return_field(inputs, schema, self.name())
    }

    fn docstring(&self) -> &'static str {
        "Extracts the number of days since the Unix epoch from the timestamp embedded in a UUIDv7."
    }
}

#[must_use]
pub fn extract_day_uuid7(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(ExtractDayUuid7, vec![input]).into()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ExtractMonthUuid7;

#[typetag::serde]
impl ScalarUDF for ExtractMonthUuid7 {
    fn name(&self) -> &'static str {
        "extract_month_uuid7"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        extract_uuid7(&input, Uuid7Unit::Month, self.name())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        uuid7_return_field(inputs, schema, self.name())
    }

    fn docstring(&self) -> &'static str {
        "Extracts the number of calendar months since 1970-01 from the timestamp embedded in a UUIDv7."
    }
}

#[must_use]
pub fn extract_month_uuid7(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(ExtractMonthUuid7, vec![input]).into()
}

fn uuid_series_from_builder(mut builder: FixedSizeBinaryBuilder) -> DaftResult<Series> {
    Ok(
        UuidArray::from_arrow(Field::new("", DataType::Uuid), Arc::new(builder.finish()))?
            .into_series(),
    )
}

fn uuid_v7_kernel(len: usize) -> DaftResult<FixedSizeBinaryBuilder> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(len, UUID_LEN as i32);

    for _ in 0..len {
        builder.append_value(RustUuid::now_v7())?;
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;

    #[test]
    fn uuid_v7_kernel_sets_version_and_variant_bits() {
        let array = uuid_v7_kernel(128).unwrap().finish();

        assert_eq!(array.len(), 128);
        for idx in 0..array.len() {
            let bytes = array.value(idx);
            assert_eq!(bytes[6] >> 4, 0x7);
            assert_eq!(bytes[8] >> 6, 0b10);
        }
    }

    #[test]
    fn uuid_v7_kernel_outputs_lexicographically_ordered_values() {
        let array = uuid_v7_kernel(128).unwrap().finish();

        for idx in 1..array.len() {
            assert!(array.value(idx - 1) < array.value(idx));
        }
    }

    /// Builds a 16-byte UUIDv7-shaped value with the given ms timestamp in the
    /// leading 48 bits. The remaining bytes (including version/variant) are set
    /// to a non-zero pattern to prove they don't affect extraction.
    fn uuid7_bytes(ms: u64) -> [u8; 16] {
        let mut b = [0xABu8; 16];
        b[0] = ((ms >> 40) & 0xFF) as u8;
        b[1] = ((ms >> 32) & 0xFF) as u8;
        b[2] = ((ms >> 24) & 0xFF) as u8;
        b[3] = ((ms >> 16) & 0xFF) as u8;
        b[4] = ((ms >> 8) & 0xFF) as u8;
        b[5] = (ms & 0xFF) as u8;
        // version 7 nibble and variant bits, as a real UUIDv7 would have
        b[6] = 0x70 | (b[6] & 0x0F);
        b[8] = 0x80 | (b[8] & 0x3F);
        b
    }

    #[test]
    fn ms_from_uuid7_reads_leading_48_bits() {
        assert_eq!(ms_from_uuid7(&uuid7_bytes(0)), 0);
        assert_eq!(ms_from_uuid7(&uuid7_bytes(1)), 1);
        // 2024-01-01T00:00:00Z
        let ms = 1_704_067_200_000;
        assert_eq!(ms_from_uuid7(&uuid7_bytes(ms)), ms as i64);
        // Maximum 48-bit value.
        let max48 = (1u64 << 48) - 1;
        assert_eq!(ms_from_uuid7(&uuid7_bytes(max48)), max48 as i64);
    }

    #[test]
    fn ms_extraction_ignores_version_and_variant_bytes() {
        // Bytes 6 and 8 carry version/variant; they must not leak into the result.
        let a = uuid7_bytes(1_704_067_200_000);
        let mut b = a;
        b[6] = 0x7F;
        b[8] = 0xBF;
        b[15] = 0x00;
        assert_eq!(ms_from_uuid7(&a), ms_from_uuid7(&b));
    }

    #[test]
    fn bucket_minute_hour_day_floor_divide() {
        // 2024-01-01T00:00:00Z = 1704067200000 ms.
        let ms = 1_704_067_200_000u64;
        let b = uuid7_bytes(ms);
        assert_eq!(bucket_uuid7(&b, Uuid7Unit::Minute), ms as i64 / 60_000);
        assert_eq!(bucket_uuid7(&b, Uuid7Unit::Hour), ms as i64 / 3_600_000);
        assert_eq!(bucket_uuid7(&b, Uuid7Unit::Day), ms as i64 / 86_400_000);
        // Epoch maps everything to 0.
        let z = uuid7_bytes(0);
        assert_eq!(bucket_uuid7(&z, Uuid7Unit::Minute), 0);
        assert_eq!(bucket_uuid7(&z, Uuid7Unit::Hour), 0);
        assert_eq!(bucket_uuid7(&z, Uuid7Unit::Day), 0);
        assert_eq!(bucket_uuid7(&z, Uuid7Unit::Month), 0);
    }

    #[test]
    fn bucket_minute_hour_day_known_values() {
        // 90 minutes after epoch.
        let ms = 90 * 60_000;
        let b = uuid7_bytes(ms);
        assert_eq!(bucket_uuid7(&b, Uuid7Unit::Minute), 90);
        assert_eq!(bucket_uuid7(&b, Uuid7Unit::Hour), 1); // floor(90/60)
        assert_eq!(bucket_uuid7(&b, Uuid7Unit::Day), 0);
    }

    #[test]
    fn bucket_month_is_calendar_months_since_epoch() {
        // 1970-01 => 0
        assert_eq!(bucket_uuid7(&uuid7_bytes(0), Uuid7Unit::Month), 0);
        // 1970-02-01T00:00:00Z = 2678400000 ms => month index 1
        assert_eq!(
            bucket_uuid7(&uuid7_bytes(2_678_400_000), Uuid7Unit::Month),
            1
        );
        // 2024-03-15T12:00:00Z => (2024-1970)*12 + (3-1) = 648 + 2 = 650
        // 2024-03-15T12:00:00Z = 1710504000000 ms
        assert_eq!(
            bucket_uuid7(&uuid7_bytes(1_710_504_000_000), Uuid7Unit::Month),
            650
        );
    }

    #[test]
    fn extract_uuid7_over_series_with_nulls() {
        let ms_a = 1_704_067_200_000u64; // 2024-01-01
        let ms_b = 1_710_504_000_000u64; // 2024-03-15
        let a = uuid7_bytes(ms_a);
        let b = uuid7_bytes(ms_b);
        let values: Vec<Option<&[u8]>> = vec![Some(a.as_slice()), None, Some(b.as_slice())];
        let arr = FixedSizeBinaryArray::from_iter("id", values.into_iter(), UUID_LEN).into_series();

        let hours = extract_uuid7(&arr, Uuid7Unit::Hour, "extract_hour_uuid7").unwrap();
        assert_eq!(hours.data_type(), &DataType::Int64);
        let got: Vec<Option<i64>> = hours.i64().unwrap().into_iter().collect();
        assert_eq!(
            got,
            vec![
                Some(ms_a as i64 / 3_600_000),
                None,
                Some(ms_b as i64 / 3_600_000)
            ]
        );
    }

    #[test]
    fn extract_uuid7_rejects_non_uuid_input() {
        let s = Int64Array::from_iter(Field::new("x", DataType::Int64), [Some(1i64)].into_iter())
            .into_series();
        let err = extract_uuid7(&s, Uuid7Unit::Hour, "extract_hour_uuid7");
        assert!(err.is_err());
    }
}
