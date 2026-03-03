use std::{ffi::CStr, sync::Arc};

use arrow_array::{Array, ArrayRef, builder::StringBuilder, cast::AsArray};
use arrow_schema::{DataType, Field};
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

    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        if args.len() != 1 {
            return Err(DaftError::TypeError(format!(
                "greet: expected 1 argument, got {}",
                args.len()
            )));
        }
        if *args[0].data_type() != DataType::Utf8 && *args[0].data_type() != DataType::LargeUtf8 {
            return Err(DaftError::TypeError(format!(
                "greet: expected string argument, got {:?}",
                args[0].data_type()
            )));
        }
        Ok(Field::new("greet", DataType::Utf8, true))
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        let names = args[0].as_string::<i64>();
        let mut builder = StringBuilder::with_capacity(names.len(), names.len() * 16);
        for i in 0..names.len() {
            if names.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(format!("Hello, {}!", names.value(i)));
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}
