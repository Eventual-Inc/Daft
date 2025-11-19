mod time;
mod to_string;
mod total;
mod truncate;
mod unix_timestamp;

use common_error::{DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, FunctionModule, FunctionRegistry, ScalarUDF, UnaryArg},
};
use serde::{Deserialize, Serialize};
use time::Time;
use truncate::Truncate;
use unix_timestamp::UnixTimestamp;

use crate::total::{
    TotalDays, TotalHours, TotalMicroseconds, TotalMilliseconds, TotalMinutes, TotalNanoseconds,
    TotalSeconds,
};

pub struct TemporalFunctions;

macro_rules! impl_temporal {
    ($name:ident, $dt:ident, $dtype:ident) => {
        paste::paste! {
            #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
            pub struct $name;

            #[typetag::serde]
            impl ScalarUDF for $name {

                fn name(&self) -> &'static str {
                    stringify!([ < $name:snake:lower > ])
                }

                fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
                    let UnaryArg {input} = inputs.try_into()?;
                    input.$dt()

                }
                fn get_return_field(
                    &self,
                    inputs: FunctionArgs<ExprRef>,
                    schema: &Schema,
                ) -> DaftResult<Field> {
                    let UnaryArg {input} = inputs.try_into()?;
                    let field = input.to_field(schema)?;
                    ensure!(
                        field.dtype.is_temporal() || field.dtype.is_time(),
                        TypeError:" Expected input to {} to be temporal, got {}",
                        self.name(),
                        field.dtype
                    );

                    Ok(Field::new(field.name, DataType::$dtype))
                }
            }
        }
    };
}
impl_temporal!(Date, dt_date, Date);
impl_temporal!(Day, dt_day, UInt32);
impl_temporal!(DayOfMonth, dt_day_of_month, UInt32);
impl_temporal!(DayOfWeek, dt_day_of_week, UInt32);
impl_temporal!(DayOfYear, dt_day_of_year, UInt32);
impl_temporal!(Hour, dt_hour, UInt32);
impl_temporal!(Microsecond, dt_microsecond, UInt32);
impl_temporal!(Millisecond, dt_millisecond, UInt32);
impl_temporal!(Minute, dt_minute, UInt32);
impl_temporal!(Month, dt_month, UInt32);
impl_temporal!(Nanosecond, dt_nanosecond, UInt32);
impl_temporal!(Quarter, dt_quarter, UInt32);
impl_temporal!(Second, dt_second, UInt32);
impl_temporal!(UnixDate, dt_unix_date, UInt64);
impl_temporal!(WeekOfYear, dt_week_of_year, UInt32);
impl_temporal!(Year, dt_year, Int32);

impl FunctionModule for TemporalFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(Date);
        parent.add_fn(Day);
        parent.add_fn(DayOfMonth);
        parent.add_fn(DayOfWeek);
        parent.add_fn(DayOfYear);
        parent.add_fn(Hour);
        parent.add_fn(Microsecond);
        parent.add_fn(Millisecond);
        parent.add_fn(Minute);
        parent.add_fn(Month);
        parent.add_fn(Nanosecond);
        parent.add_fn(Quarter);
        parent.add_fn(Second);
        parent.add_fn(Time);
        parent.add_fn(to_string::ToString);
        parent.add_fn(Truncate);
        parent.add_fn(TotalDays);
        parent.add_fn(TotalHours);
        parent.add_fn(TotalMicroseconds);
        parent.add_fn(TotalMilliseconds);
        parent.add_fn(TotalMinutes);
        parent.add_fn(TotalNanoseconds);
        parent.add_fn(TotalSeconds);
        parent.add_fn(UnixDate);
        parent.add_fn(WeekOfYear);
        parent.add_fn(Year);
        parent.add_fn(UnixTimestamp);
    }
}
