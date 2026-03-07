use arrow::ffi as arrow_ffi;
use arrow_array::{ArrayRef, make_array};

use crate::{FFI_ArrowArray, FFI_ArrowSchema};

/// Export a single Arrow array to FFI structs.
pub fn export_arrow_array(array: &ArrayRef) -> Result<(FFI_ArrowArray, FFI_ArrowSchema), String> {
    arrow_ffi::to_ffi(&array.to_data()).map_err(|e| format!("Arrow FFI export failed: {e}"))
}

/// Import a single Arrow array from FFI structs.
///
/// # Safety
///
/// `array` must be a valid `FFI_ArrowArray` (ownership is taken).
/// `schema` must point to a valid `FFI_ArrowSchema` (borrowed).
pub unsafe fn import_arrow_array(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
) -> Result<ArrayRef, String> {
    let data = unsafe { arrow_ffi::from_ffi(array, schema) }
        .map_err(|e| format!("Arrow FFI import failed: {e}"))?;
    Ok(make_array(data))
}

/// Import Arrow arrays from C Data Interface pointers.
///
/// # Safety
///
/// - `args` must point to `count` valid `FFI_ArrowArray` values (ownership taken).
/// - `schemas` must point to `count` valid `FFI_ArrowSchema` values (borrowed).
pub unsafe fn import_arrow_args(
    args: *const FFI_ArrowArray,
    schemas: *const FFI_ArrowSchema,
    count: usize,
) -> Result<Vec<ArrayRef>, String> {
    let mut arrays = Vec::with_capacity(count);
    for i in 0..count {
        let ffi_array = unsafe { std::ptr::read(args.add(i)) };
        let ffi_schema = unsafe { &*schemas.add(i) };
        let arr = unsafe { import_arrow_array(ffi_array, ffi_schema) }
            .map_err(|e| format!("arg {i}: {e}"))?;
        arrays.push(arr);
    }
    Ok(arrays)
}

/// Export an Arrow array through C Data Interface out-pointers.
///
/// # Safety
///
/// `ret_array` and `ret_schema` must be valid, writable pointers.
pub unsafe fn export_arrow_result(
    array: ArrayRef,
    ret_array: *mut FFI_ArrowArray,
    ret_schema: *mut FFI_ArrowSchema,
) -> Result<(), String> {
    let (out_array, out_schema) = export_arrow_array(&array)?;
    unsafe {
        std::ptr::write(ret_array, out_array);
        std::ptr::write(ret_schema, out_schema);
    }
    Ok(())
}
