use arrow::ffi as arrow_ffi;
use arrow_array::{ArrayRef, make_array};
use daft_ext_abi::{FFI_ArrowArray, FFI_ArrowSchema};

use crate::error::{DaftError, DaftResult};

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
) -> DaftResult<Vec<ArrayRef>> {
    let mut arrays = Vec::with_capacity(count);
    for i in 0..count {
        let ffi_array = unsafe { std::ptr::read(args.add(i)) };
        let ffi_schema = unsafe { &*schemas.add(i) };
        let data = unsafe { arrow_ffi::from_ffi(ffi_array, ffi_schema) }
            .map_err(|e| DaftError::RuntimeError(format!("failed to import array {i}: {e}")))?;
        arrays.push(make_array(data));
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
) -> DaftResult<()> {
    let (out_array, out_schema) = arrow_ffi::to_ffi(&array.to_data())
        .map_err(|e| DaftError::RuntimeError(format!("failed to export result: {e}")))?;
    unsafe {
        std::ptr::write(ret_array, out_array);
        std::ptr::write(ret_schema, out_schema);
    }
    Ok(())
}
