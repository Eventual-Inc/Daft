use arrow_array::ArrayRef;
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
    unsafe { daft_ext_abi::ffi::arrow::import_arrow_args(args, schemas, count) }
        .map_err(DaftError::RuntimeError)
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
    unsafe { daft_ext_abi::ffi::arrow::export_arrow_result(array, ret_array, ret_schema) }
        .map_err(DaftError::RuntimeError)
}
