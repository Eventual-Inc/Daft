use std::ops::{Add, Sub};

use daft_core::datatypes::Int32Type;
use daft_dsl::functions::prelude::*;

// --- DateAdd ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DateAdd;

#[derive(FunctionArgs)]
struct DateAddArgs<T> {
    input: T,
    days: T,
}

#[typetag::serde]
impl ScalarUDF for DateAdd {
    fn name(&self) -> &'static str {
        "date_add"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let DateAddArgs { input, days } = inputs.try_into()?;
        let date_series = input.cast(&DataType::Date)?;
        let days_i32 = days.cast(&DataType::Int32)?;
        let days_arr = days_i32.downcast::<daft_core::array::DataArray<Int32Type>>()?;
        let result = date_series.date()?.physical.add(days_arr)?;
        result.cast(&DataType::Date)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let DateAddArgs { input, days } = inputs.try_into()?;
        let input_field = input.to_field(schema)?;
        let days_field = days.to_field(schema)?;
        ensure!(
            matches!(input_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp input, got {}",
            input_field.dtype
        );
        ensure!(
            days_field.dtype.is_integer(),
            TypeError: "Expected integer for days, got {}",
            days_field.dtype
        );
        Ok(Field::new(input_field.name, DataType::Date))
    }
}

// --- DateSub ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DateSub;

#[typetag::serde]
impl ScalarUDF for DateSub {
    fn name(&self) -> &'static str {
        "date_sub"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let DateAddArgs { input, days } = inputs.try_into()?;
        let date_series = input.cast(&DataType::Date)?;
        let days_i32 = days.cast(&DataType::Int32)?;
        let days_arr = days_i32.downcast::<daft_core::array::DataArray<Int32Type>>()?;
        let result = date_series.date()?.physical.sub(days_arr)?;
        result.cast(&DataType::Date)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let DateAddArgs { input, days } = inputs.try_into()?;
        let input_field = input.to_field(schema)?;
        let days_field = days.to_field(schema)?;
        ensure!(
            matches!(input_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp input, got {}",
            input_field.dtype
        );
        ensure!(
            days_field.dtype.is_integer(),
            TypeError: "Expected integer for days, got {}",
            days_field.dtype
        );
        Ok(Field::new(input_field.name, DataType::Date))
    }
}

// --- DateDiff ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DateDiff;

#[derive(FunctionArgs)]
struct DateDiffArgs<T> {
    end_date: T,
    start_date: T,
}

#[typetag::serde]
impl ScalarUDF for DateDiff {
    fn name(&self) -> &'static str {
        "date_diff"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let DateDiffArgs {
            end_date,
            start_date,
        } = inputs.try_into()?;
        let end_series = end_date.cast(&DataType::Date)?;
        let start_series = start_date.cast(&DataType::Date)?;
        let result = end_series
            .date()?
            .physical
            .sub(&start_series.date()?.physical)?;
        result.cast(&DataType::Int32)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let DateDiffArgs {
            end_date,
            start_date,
        } = inputs.try_into()?;
        let end_field = end_date.to_field(schema)?;
        let start_field = start_date.to_field(schema)?;
        ensure!(
            matches!(end_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp for end_date, got {}",
            end_field.dtype
        );
        ensure!(
            matches!(start_field.dtype, DataType::Date | DataType::Timestamp(..)),
            TypeError: "Expected date or timestamp for start_date, got {}",
            start_field.dtype
        );
        Ok(Field::new(end_field.name, DataType::Int32))
    }
}
