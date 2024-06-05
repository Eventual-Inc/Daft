use arrow2::array::{Array, PrimitiveArray};
use arrow2::datatypes::Field;
use arrow2::error::Result;
use arrow2::ffi;

fn export(array: Box<dyn Array>) -> (ffi::ArrowArray, ffi::ArrowSchema) {
    // importing an array requires an associated field so that the consumer knows its datatype.
    // Thus, we need to export both
    let field = Field::new("a", array.data_type().clone(), true);
    (
        ffi::export_array_to_c(array),
        ffi::export_field_to_c(&field),
    )
}

/// # Safety
/// `ArrowArray` and `ArrowSchema` must be valid
unsafe fn import(array: ffi::ArrowArray, schema: &ffi::ArrowSchema) -> Result<Box<dyn Array>> {
    let field = ffi::import_field_from_c(schema)?;
    ffi::import_array_from_c(array, field.data_type)
}

fn main() -> Result<()> {
    // let's assume that we have an array:
    let array = PrimitiveArray::<i32>::from([Some(1), None, Some(123)]).boxed();

    // here we export - `array_ffi` and `schema_ffi` are the structs of the C data interface
    let (array_ffi, schema_ffi) = export(array.clone());

    // here we import them. Often the structs are wrapped in a pointer. In that case you
    // need to read the pointer to the stack.

    // Safety: we used `export`, which is a valid exporter to the C data interface
    let new_array = unsafe { import(array_ffi, &schema_ffi)? };

    // which is equal to the exported array
    assert_eq!(array.as_ref(), new_array.as_ref());
    Ok(())
}
