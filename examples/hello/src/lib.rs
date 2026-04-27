use std::{ffi::CStr, sync::Arc};

use arrow::{
    array::{Array, builder::StringBuilder, cast::AsArray},
    datatypes::{DataType, Field},
};
use daft_ext::{daft_extension, prelude::*};

// ── Module ──────────────────────────────────────────────────────────

#[daft_extension]
struct HelloExtension;

impl DaftExtension for HelloExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(Greet));
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
