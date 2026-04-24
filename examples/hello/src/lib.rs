use std::{ffi::CStr, sync::Arc};

use arrow::{
    array::{Array, builder::StringBuilder, cast::AsArray},
    datatypes::{DataType, Field},
};
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

// ── Function ────────────────────────────────────────────────────────

struct Greet;

impl DaftScalarFunction for Greet {
    fn name(&self) -> &CStr {
        c"greet"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        if args.len() != 1 {
            return Err(DaftError::TypeError(format!(
                "greet: expected 1 argument, got {}",
                args.len()
            )));
        }
        let field = Field::try_from(&args[0])?;
        let dt = field.data_type();
        if *dt != DataType::Utf8 && *dt != DataType::LargeUtf8 {
            return Err(DaftError::TypeError(format!(
                "greet: expected string argument, got {:?}",
                dt
            )));
        }
        Ok(ArrowSchema::try_from(&Field::new(
            "greet",
            DataType::Utf8,
            true,
        ))?)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        // Extract the first (and only) argument from the function call, this is the mutual type.
        let data = args.into_iter().next().unwrap();

        // Convert the ArrowData input structs to FFI-compatible Arrow array and schema so we can get it into the arrow-rs library.
        let ffi_array: arrow::ffi::FFI_ArrowArray = data.array.into();
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = data.schema.into();

        // Now make it into an arrow-rs string array.
        let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }?;
        let input = arrow::array::make_array(arrow_data);
        let names = input.as_string::<i64>();

        // Actually do the computation: build a result array of "Hello, <name>!" strings.
        let mut builder = StringBuilder::with_capacity(names.len(), names.len() * 16);
        for i in 0..names.len() {
            if names.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(format!("Hello, {}!", names.value(i)));
            }
        }
        let output = builder.finish();

        // Convert the result array back to FFI-compatible objects required by Daft.
        let (out_arr, out_sch) = arrow::ffi::to_ffi(&output.to_data())?;

        // Wrap as Daft's ArrowData for integration with Python and Daft machinery.
        Ok(ArrowData {
            array: out_arr.into(),
            schema: out_sch.into(),
        })
    }
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
        Ok(ArrowSchema::try_from(&Field::new(
            "string_count",
            DataType::Int64,
            false,
        ))?)
    }

    fn state_fields(&self, _args: &[ArrowSchema]) -> DaftResult<Vec<ArrowSchema>> {
        Ok(vec![ArrowSchema::try_from(&Field::new(
            "count",
            DataType::Int64,
            false,
        ))?])
    }

    fn agg_block(&self, inputs: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
        let data = inputs.into_iter().next().unwrap();
        let ffi_array: arrow::ffi::FFI_ArrowArray = data.array.into();
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = data.schema.into();
        let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }?;
        let input = arrow::array::make_array(arrow_data);

        let non_null_count = input.len() - input.null_count();
        let count_array = arrow::array::Int64Array::from(vec![non_null_count as i64]);

        let (out_arr, out_sch) = arrow::ffi::to_ffi(&count_array.to_data())?;
        Ok(vec![ArrowData {
            array: out_arr.into(),
            schema: out_sch.into(),
        }])
    }

    fn combine(&self, states: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
        let data = states.into_iter().next().unwrap();
        let ffi_array: arrow::ffi::FFI_ArrowArray = data.array.into();
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = data.schema.into();
        let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }?;
        let counts = arrow::array::make_array(arrow_data);
        let counts = counts
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        let total: i64 = (0..counts.len())
            .filter(|i| !counts.is_null(*i))
            .map(|i| counts.value(i))
            .sum();

        let result = arrow::array::Int64Array::from(vec![total]);
        let (out_arr, out_sch) = arrow::ffi::to_ffi(&result.to_data())?;
        Ok(vec![ArrowData {
            array: out_arr.into(),
            schema: out_sch.into(),
        }])
    }

    fn finalize(&self, states: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let data = states.into_iter().next().unwrap();
        let ffi_array: arrow::ffi::FFI_ArrowArray = data.array.into();
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = data.schema.into();
        let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }?;
        let counts = arrow::array::make_array(arrow_data);
        let counts = counts
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        let total: i64 = counts.value(0);
        let result = arrow::array::Int64Array::from(vec![total]);
        let (out_arr, out_sch) = arrow::ffi::to_ffi(&result.to_data())?;
        Ok(ArrowData {
            array: out_arr.into(),
            schema: out_sch.into(),
        })
    }
}
