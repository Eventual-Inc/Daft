use std::{ffi::CStr, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Int64Array,
    builder::{FixedSizeListBuilder, Int64Builder},
};
use arrow_schema::{DataType, Field};
use daft_ext::{daft_extension, prelude::*};

// ── Module ──────────────────────────────────────────────────────────

#[daft_extension]
struct HelloExtension;

impl DaftExtension for HelloExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(Greet));
        session.define_function(Arc::new(Splat));
        session.define_aggregate_function(Arc::new(StringCount));
    }
}

// ── Scalar Function ────────────────────────────────────────────────

#[daft_func]
fn greet(name: &str) -> String {
    format!("Hello, {}!", name)
}

// ── Scalar Function with a value-dependent output type ─────────────

/// The most one call may produce, so a typo in a query can't ask for a
/// terabyte-wide column.
const SPLAT_MAX_COUNT: i64 = 1 << 20;

/// `splat(value, count)` repeats each value into a `FixedSizeList` of length
/// `count`. The *width of the output type* comes from the value of `count`,
/// which is only knowable because planning hands us foldable literals.
struct Splat;

impl DaftScalarFunction for Splat {
    fn name(&self) -> &CStr {
        c"splat"
    }

    fn return_field(&self, args: &[ArgDescriptor]) -> DaftResult<ArrowSchema> {
        if args.len() != 2 {
            return Err(DaftError::TypeError(format!(
                "splat: expected 2 arguments, got {}",
                args.len()
            )));
        }

        let value_field = import_field(args[0].field())?;
        if *value_field.data_type() != DataType::Int64 {
            return Err(DaftError::TypeError(format!(
                "splat: expected an Int64 first argument, got {:?}",
                value_field.data_type()
            )));
        }

        // `count` is only present when the argument folds to a constant; a
        // column here is a planning error, not a runtime one.
        let count = with_literal(&args[1], read_count)?.ok_or_else(|| {
            DaftError::TypeError("splat: 'count' must be a literal, not a column".to_string())
        })?;

        let item = Field::new("item", DataType::Int64, true);
        export_field(&Field::new(
            "splat",
            DataType::FixedSizeList(Arc::new(item), count as i32),
            true,
        ))
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        if args.len() != 2 {
            return Err(DaftError::TypeError(format!(
                "splat: expected 2 arguments in call, got {}",
                args.len()
            )));
        }
        let mut args = args.into_iter();
        let values = import_array(args.next().expect("checked above"))?;
        let counts = import_array(args.next().expect("checked above"))?;

        let values = downcast_i64(&values, "value")?;
        let count = read_count(&counts)?;

        let mut builder =
            FixedSizeListBuilder::new(Int64Builder::new(), count as i32).with_field(Arc::new(
                Field::new("item", DataType::Int64, true),
            ));
        for i in 0..values.len() {
            let value = (!values.is_null(i)).then(|| values.value(i));
            for _ in 0..count {
                builder.values().append_option(value);
            }
            builder.append(value.is_some());
        }

        export_array(Arc::new(builder.finish()), "splat")
    }
}

fn downcast_i64<'a>(array: &'a ArrayRef, what: &str) -> DaftResult<&'a Int64Array> {
    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        DaftError::TypeError(format!(
            "splat: expected an Int64 {what}, got {:?}",
            array.data_type()
        ))
    })
}

/// Read and validate the repeat count from a length-1 array.
fn read_count(array: &ArrayRef) -> DaftResult<i64> {
    let counts = downcast_i64(array, "count")?;
    if counts.is_empty() || counts.is_null(0) {
        return Err(DaftError::TypeError(
            "splat: 'count' must not be null".to_string(),
        ));
    }
    let count = counts.value(0);
    if count <= 0 || count > SPLAT_MAX_COUNT {
        return Err(DaftError::TypeError(format!(
            "splat: 'count' must be between 1 and {SPLAT_MAX_COUNT}, got {count}"
        )));
    }
    Ok(count)
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
