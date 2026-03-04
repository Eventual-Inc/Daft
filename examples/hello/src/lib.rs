use std::{ffi::CStr, sync::Arc};

use arrow::{
    array::{Array, builder::StringBuilder, cast::AsArray},
    datatypes::{DataType, Field},
};
use daft_ext::prelude::*;

daft_ext::define_arrow_helpers!();

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
        let field = import_field(&args[0])?;
        let dt = field.data_type();
        if *dt != DataType::Utf8 && *dt != DataType::LargeUtf8 {
            return Err(DaftError::TypeError(format!(
                "greet: expected string argument, got {:?}",
                dt
            )));
        }
        Ok(export_field(&Field::new("greet", DataType::Utf8, true))?)
    }

    fn call(&self, args: &[ArrowData]) -> DaftResult<ArrowData> {
        let input = import_array(unsafe { ArrowData::take_arg(args, 0) })?;
        let names = input.as_string::<i64>();
        let mut builder = StringBuilder::with_capacity(names.len(), names.len() * 16);
        for i in 0..names.len() {
            if names.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(format!("Hello, {}!", names.value(i)));
            }
        }
        Ok(export_array(&builder.finish())?)
    }
}
