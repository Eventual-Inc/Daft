use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{Schema, Series},
    series::IntoSeries,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

macro_rules! impl_total {
    ($name:ident, $cast_method:ident) => {
        paste::paste! {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $name;

        #[typetag::serde]
        impl ScalarUDF for $name {
            fn name(&self) -> &'static str {
                stringify!([ < $name:snake:lower > ])
            }

            fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
                ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
                let s = inputs.required((0, "input"))?;
                match s.data_type() {
                    DataType::Duration(_) => {
                        Ok(s.duration()?.$cast_method()?.into_series())
                    },
                    dt => Err(DaftError::TypeError(format!("{} not implemented for {}", self.name(), dt)))
                }
            }

            fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
                ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
                let input = inputs.required((0, "input"))?.to_field(schema)?;
                ensure!(input.dtype.is_duration(), "expected duration");
                Ok(Field::new(input.name, DataType::Int64))
            }
        }
    }
    };
}

impl_total!(TotalSeconds, cast_to_seconds);
impl_total!(TotalMilliseconds, cast_to_milliseconds);
impl_total!(TotalMicroseconds, cast_to_microseconds);
impl_total!(TotalNanoseconds, cast_to_nanoseconds);
impl_total!(TotalMinutes, cast_to_minutes);
impl_total!(TotalHours, cast_to_hours);
impl_total!(TotalDays, cast_to_days);
