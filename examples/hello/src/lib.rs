use std::{ffi::CStr, sync::Arc};

use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use daft_ext::prelude::*;

// ── Module ──────────────────────────────────────────────────────────

#[daft_extension]
struct HelloExtension;

impl DaftExtension for HelloExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(Greet));
        session.define_aggregate_function(Arc::new(StringCount));
    }
}

// ── Scalar Function (row-level macro) ──────────────────────────────

#[daft_func]
fn greet(name: &str) -> String {
    format!("Hello, {}!", name)
}

// ── Aggregate Function ─────────────────────────────────────────────

struct StringCount;

impl DaftAggregateFunction for StringCount {
    fn name(&self) -> &CStr {
        c"string_count"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        if args.len() != 1 {
            return Err(DaftError::TypeError(format!(
                "string_count: expected 1 argument, got {}",
                args.len()
            )));
        }
        export_field(&Field::new("string_count", DataType::Int64, false))
    }

    fn state_fields(&self, _args: &[ArrowSchema]) -> DaftResult<Vec<ArrowSchema>> {
        Ok(vec![export_field(&Field::new(
            "count",
            DataType::Int64,
            false,
        ))?])
    }

    fn aggregate(&self, inputs: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
        let input = import_array(inputs.into_iter().next().unwrap())?;
        let non_null_count = input.len() - input.null_count();
        let result: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![non_null_count as i64]));
        Ok(vec![export_array(result, "count")?])
    }

    fn combine(&self, states: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
        let counts = import_array(states.into_iter().next().unwrap())?;
        let counts = counts
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();

        let total: i64 = (0..counts.len())
            .filter(|i| !counts.is_null(*i))
            .map(|i| counts.value(i))
            .sum();

        let result: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![total]));
        Ok(vec![export_array(result, "count")?])
    }

    fn finalize(&self, states: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let counts = import_array(states.into_iter().next().unwrap())?;
        let counts = counts
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();

        let total: i64 = counts.value(0);
        let result: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![total]));
        export_array(result, "string_count")
    }
}
