use arrow_array::Int32Array;
use arrow_schema::DataType;
use daft_ext::prelude::*;

/// Example Daft extension that registers one scalar function.
#[daft_extension]
struct ExampleExtension;

impl DaftExtension for ExampleExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(IncrementFn));
    }
}

/// A scalar function that increments every element of an Int32 column by 1.
struct IncrementFn;

impl DaftScalarFunction for IncrementFn {
    fn name(&self) -> &CStr {
        c"increment"
    }

    fn return_field(&self, _args: &[Field]) -> DaftResult<Field> {
        Ok(Field::new("result", DataType::Int32, false))
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        let input = args[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| DaftError::TypeError("expected Int32".into()))?;
        let output: Int32Array = input.iter().map(|v| v.map(|x| x + 1)).collect();
        Ok(Arc::new(output))
    }
}
