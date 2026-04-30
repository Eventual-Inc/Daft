use std::sync::Arc;

use arrow_array::Date32Array;
use chrono::{Datelike, NaiveDate, Weekday};
use daft_core::prelude::AsArrow;
use daft_dsl::functions::prelude::*;

const UNIX_EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => unreachable!(),
};

fn days_to_date(days: i32) -> NaiveDate {
    UNIX_EPOCH + chrono::Duration::days(days as i64)
}

fn date_to_days(date: NaiveDate) -> i32 {
    date.signed_duration_since(UNIX_EPOCH).num_days() as i32
}

// --- LastDay ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LastDay;

#[typetag::serde]
impl ScalarUDF for LastDay {
    fn name(&self) -> &'static str {
        "last_day"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let daft_dsl::functions::UnaryArg { input } = inputs.try_into()?;
        let date_series = input.cast(&DataType::Date)?;
        let date_arr = date_series.date()?;
        let arr = date_arr.as_arrow()?;

        let values: Vec<Option<i32>> = arr
            .iter()
            .map(|opt_days| {
                opt_days.and_then(|days| {
                    let d = days_to_date(days);
                    let (y, m) = (d.year(), d.month());
                    let first_of_next = if m == 12 {
                        NaiveDate::from_ymd_opt(y + 1, 1, 1)
                    } else {
                        NaiveDate::from_ymd_opt(y, m + 1, 1)
                    }?;
                    let last = first_of_next - chrono::Duration::days(1);
                    Some(date_to_days(last))
                })
            })
            .collect();

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Date32Array::from(values));
        Series::from_arrow(
            Arc::new(Field::new(date_arr.name().to_string(), DataType::Date)),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let daft_dsl::functions::UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            matches!(field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp input, got {}",
            field.dtype
        );
        Ok(Field::new(field.name, DataType::Date))
    }
}

// --- NextDay ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NextDay;

#[derive(FunctionArgs)]
struct NextDayArgs<T> {
    input: T,
    day_of_week: String,
}

fn parse_weekday(s: &str) -> DaftResult<Weekday> {
    match s.to_lowercase().as_str() {
        "monday" | "mon" | "mo" => Ok(Weekday::Mon),
        "tuesday" | "tue" | "tu" => Ok(Weekday::Tue),
        "wednesday" | "wed" | "we" => Ok(Weekday::Wed),
        "thursday" | "thu" | "th" => Ok(Weekday::Thu),
        "friday" | "fri" | "fr" => Ok(Weekday::Fri),
        "saturday" | "sat" | "sa" => Ok(Weekday::Sat),
        "sunday" | "sun" | "su" => Ok(Weekday::Sun),
        _ => Err(daft_common::error::DaftError::ValueError(format!(
            "Invalid day of week: '{s}'. Expected: Monday/Mon, Tuesday/Tue, etc."
        ))),
    }
}

#[typetag::serde]
impl ScalarUDF for NextDay {
    fn name(&self) -> &'static str {
        "next_day"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let NextDayArgs { input, day_of_week } = inputs.try_into()?;
        let target_weekday = parse_weekday(&day_of_week)?;

        let date_series = input.cast(&DataType::Date)?;
        let date_arr = date_series.date()?;
        let arr = date_arr.as_arrow()?;

        let values: Vec<Option<i32>> = arr
            .iter()
            .map(|opt_days| {
                opt_days.map(|days| {
                    let d = days_to_date(days);
                    // Advance 1-7 days to find the next occurrence of target_weekday
                    let mut next = d + chrono::Duration::days(1);
                    while next.weekday() != target_weekday {
                        next += chrono::Duration::days(1);
                    }
                    date_to_days(next)
                })
            })
            .collect();

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Date32Array::from(values));
        Series::from_arrow(
            Arc::new(Field::new(date_arr.name().to_string(), DataType::Date)),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let NextDayArgs { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            matches!(field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp input, got {}",
            field.dtype
        );
        Ok(Field::new(field.name, DataType::Date))
    }
}
