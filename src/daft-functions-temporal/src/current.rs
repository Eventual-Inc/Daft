use std::sync::Arc;

use arrow_array::{builder::LargeStringBuilder, Date32Array, TimestampMicrosecondArray};
use daft_core::datatypes::TimeUnit;
use daft_dsl::functions::prelude::*;

// --- CurrentDate ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CurrentDate;

#[typetag::serde]
impl ScalarUDF for CurrentDate {
    fn name(&self) -> &'static str {
        "current_date"
    }

    fn call(
        &self,
        _inputs: FunctionArgs<Series>,
        ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let len = ctx.row_count;
        let today = chrono::Utc::now().date_naive();
        let days_since_epoch = today.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
        let arrow_arr: arrow_array::ArrayRef = Arc::new(Date32Array::from(vec![days_since_epoch; len]));
        Series::from_arrow(Arc::new(Field::new("", DataType::Date)), arrow_arr)
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.is_empty(), ValueError: "Expected 0 input args, got {}", inputs.len());
        Ok(Field::new("", DataType::Date))
    }
}

// --- CurrentTimestamp ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CurrentTimestamp;

#[typetag::serde]
impl ScalarUDF for CurrentTimestamp {
    fn name(&self) -> &'static str {
        "current_timestamp"
    }

    fn call(
        &self,
        _inputs: FunctionArgs<Series>,
        ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let len = ctx.row_count;
        let now_us = chrono::Utc::now().timestamp_micros();
        let arrow_arr: arrow_array::ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![now_us; len]));
        Series::from_arrow(
            Arc::new(Field::new("", DataType::Timestamp(TimeUnit::Microseconds, None))),
            arrow_arr,
        )
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.is_empty(), ValueError: "Expected 0 input args, got {}", inputs.len());
        Ok(Field::new("", DataType::Timestamp(TimeUnit::Microseconds, None)))
    }
}

// --- CurrentTimezone ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CurrentTimezone;

#[typetag::serde]
impl ScalarUDF for CurrentTimezone {
    fn name(&self) -> &'static str {
        "current_timezone"
    }

    fn call(
        &self,
        _inputs: FunctionArgs<Series>,
        ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let len = ctx.row_count;
        let mut builder = LargeStringBuilder::with_capacity(len, len * 3);
        for _ in 0..len {
            builder.append_value("UTC");
        }
        let arrow_arr: arrow_array::ArrayRef = Arc::new(builder.finish());
        Series::from_arrow(Arc::new(Field::new("", DataType::Utf8)), arrow_arr)
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.is_empty(), ValueError: "Expected 0 input args, got {}", inputs.len());
        Ok(Field::new("", DataType::Utf8))
    }
}
