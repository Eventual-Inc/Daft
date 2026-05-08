use std::{
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow_array::builder::FixedSizeBinaryBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, FromArrow, Schema, UuidArray},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{
        FunctionArgs, ScalarUDF,
        scalar::{EvalContext, ScalarFn},
    },
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use uuid::Uuid as RustUuid;

const UUID_LEN: i32 = 16;
const UUID_V7_VERSION: u8 = 0x70;
const UUID_RFC4122_VARIANT: u8 = 0x80;
const UUID_V7_SEQUENCE_MAX: u16 = 0x0fff;

static UUID_V7_STATE: LazyLock<Mutex<UuidV7State>> =
    LazyLock::new(|| Mutex::new(UuidV7State::default()));

#[derive(Default)]
struct UuidV7State {
    last_unix_ts_ms: u64,
    sequence: u16,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Uuid;

#[typetag::serde]
impl ScalarUDF for Uuid {
    fn name(&self) -> &'static str {
        "uuid"
    }

    fn call(&self, _inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let len = ctx.row_count;

        let mut builder = FixedSizeBinaryBuilder::with_capacity(len, UUID_LEN);
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

fn uuid_series_from_builder(mut builder: FixedSizeBinaryBuilder) -> DaftResult<Series> {
    Ok(
        UuidArray::from_arrow(Field::new("", DataType::Uuid), Arc::new(builder.finish()))?
            .into_series(),
    )
}

fn uuid_v7_kernel(len: usize) -> DaftResult<FixedSizeBinaryBuilder> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(len, UUID_LEN);
    let mut rng = rand::rng();

    for _ in 0..len {
        let (unix_ts_ms, sequence) = next_uuid_v7_timestamp_and_sequence()?;
        let mut bytes = [0u8; UUID_LEN as usize];
        rng.fill_bytes(&mut bytes[8..]);

        bytes[0] = (unix_ts_ms >> 40) as u8;
        bytes[1] = (unix_ts_ms >> 32) as u8;
        bytes[2] = (unix_ts_ms >> 24) as u8;
        bytes[3] = (unix_ts_ms >> 16) as u8;
        bytes[4] = (unix_ts_ms >> 8) as u8;
        bytes[5] = unix_ts_ms as u8;
        bytes[6] = UUID_V7_VERSION | ((sequence >> 8) as u8 & 0x0f);
        bytes[7] = sequence as u8;
        bytes[8] = UUID_RFC4122_VARIANT | (bytes[8] & 0x3f);

        builder.append_value(bytes)?;
    }

    Ok(builder)
}

fn next_uuid_v7_timestamp_and_sequence() -> DaftResult<(u64, u16)> {
    let mut state = UUID_V7_STATE
        .lock()
        .map_err(|_| DaftError::External("UUIDv7 generator state mutex was poisoned".into()))?;

    loop {
        let unix_ts_ms = current_unix_timestamp_ms()?;
        if unix_ts_ms > state.last_unix_ts_ms {
            state.last_unix_ts_ms = unix_ts_ms;
            state.sequence = 0;
            return Ok((unix_ts_ms, state.sequence));
        }

        if state.sequence < UUID_V7_SEQUENCE_MAX {
            state.sequence += 1;
            return Ok((state.last_unix_ts_ms, state.sequence));
        }

        std::thread::sleep(Duration::from_micros(100));
    }
}

fn current_unix_timestamp_ms() -> DaftResult<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            DaftError::ValueError(format!("System clock is before the Unix epoch: {err}"))
        })?;
    u64::try_from(duration.as_millis()).map_err(|_| {
        DaftError::ValueError("Current Unix timestamp does not fit in u64 milliseconds".into())
    })
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
}
