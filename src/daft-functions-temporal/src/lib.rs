use daft_dsl::functions::{FunctionModule, FunctionRegistry};

use crate::total::{
    TotalDays, TotalHours, TotalMicroseconds, TotalMilliseconds, TotalMinutes, TotalNanoseconds,
    TotalSeconds,
};

mod total;

pub struct TemporalFunctions;

impl FunctionModule for TemporalFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(TotalSeconds);
        parent.add_fn(TotalMilliseconds);
        parent.add_fn(TotalMicroseconds);
        parent.add_fn(TotalNanoseconds);
        parent.add_fn(TotalMinutes);
        parent.add_fn(TotalHours);
        parent.add_fn(TotalDays);
    }
}
