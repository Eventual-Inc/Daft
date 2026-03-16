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
        let data = args.into_iter().next().unwrap();
        let ffi_array: arrow::ffi::FFI_ArrowArray = data.array.into();
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = data.schema.into();
        let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }?;
        let input = arrow::array::make_array(arrow_data);
        let names = input.as_string::<i64>();
        let mut builder = StringBuilder::with_capacity(names.len(), names.len() * 16);
        for i in 0..names.len() {
            if names.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(format!("Hello, {}!", names.value(i)));
            }
        }
        let output = builder.finish();
        let (out_arr, out_sch) = arrow::ffi::to_ffi(&output.to_data())?;
        Ok(ArrowData {
            array: out_arr.into(),
            schema: out_sch.into(),
        })
    }
}
