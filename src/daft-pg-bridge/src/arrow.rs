//! Arrow C Data import with transactional ownership transfer.
//!
//! Import first builds a Daft-owned `MicroPartition` while the caller still
//! owns the producer callbacks.  Only after every validation and conversion
//! succeeds are the two C structs moved into an ownership guard and the
//! caller-visible release pointers cleared.  The guard is retained by the
//! Arrow buffers and invokes each producer callback exactly once when the last
//! Daft reference is dropped.

use std::{ffi::CStr, ptr, sync::Arc};

use arrow::{
    array::{make_array, Array, MutableArrayData, StructArray},
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use daft_core::prelude::Schema;
use daft_micropartition::{MicroPartition, MicroPartitionRef};

use crate::error::{DpgError, DpgErrorCode};

/// ABI-compatible Arrow C Data schema.
pub type ArrowSchema = FFI_ArrowSchema;

/// ABI-compatible Arrow C Data array.
pub type ArrowArray = FFI_ArrowArray;

/// C-facing import status used by the low-level ownership test entrypoint.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowImportResult {
    /// Import succeeded and ownership transferred to Daft.
    Ok = 0,
    /// Capacity is unavailable; caller retains ownership and may retry.
    Again = 1,
    /// Validation or conversion failed; caller retains ownership.
    Error = 2,
}

/// Admission decision made before Arrow ownership can transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowImportAdmission {
    /// The consumer has reserved capacity for this batch.
    Ready,
    /// The bounded consumer queue is full.
    Again,
}

/// Rust result of a capacity-aware import attempt.
#[derive(Debug)]
pub enum ArrowBatchImport {
    /// A fully owned Daft batch.
    Imported(MicroPartitionRef),
    /// No ownership transfer occurred.
    Again,
    /// No ownership transfer occurred.
    Error(String),
}

unsafe extern "C" fn release_borrowed_view(array: *mut FFI_ArrowArray) {
    if array.is_null() {
        return;
    }
    unsafe { (*array).release = None };
}

unsafe fn validate_schema(schema: *const ArrowSchema) -> Result<(), String> {
    if schema.is_null() {
        return Err("schema is NULL".to_string());
    }
    let schema = unsafe { &*schema };
    if schema.release.is_none() {
        return Err("schema has no release callback".to_string());
    }
    if schema.format.is_null() {
        return Err("schema format is NULL".to_string());
    }
    unsafe { CStr::from_ptr(schema.format) }
        .to_str()
        .map_err(|_| "schema format is not UTF-8".to_string())?;
    if schema.n_children < 0 {
        return Err("schema child count is negative".to_string());
    }
    if schema.n_children > 0 && schema.children.is_null() {
        return Err("schema children pointer is NULL".to_string());
    }
    Ok(())
}

unsafe fn validate_array(array: *const ArrowArray) -> Result<(), String> {
    if array.is_null() {
        return Err("array is NULL".to_string());
    }
    let array = unsafe { &*array };
    if array.release.is_none() {
        return Err("array has no release callback".to_string());
    }
    if array.length < 0 {
        return Err("array length is negative".to_string());
    }
    // Arrow permits -1 to mean an unknown null count.
    if array.null_count < -1 {
        return Err("array null count is below -1".to_string());
    }
    if array.offset < 0 {
        return Err("array offset is negative".to_string());
    }
    if array.n_buffers < 0 || array.n_children < 0 {
        return Err("array buffer or child count is negative".to_string());
    }
    if array.n_buffers > 0 && array.buffers.is_null() {
        return Err("array buffers pointer is NULL".to_string());
    }
    if array.n_children > 0 && array.children.is_null() {
        return Err("array children pointer is NULL".to_string());
    }
    Ok(())
}

unsafe fn import_ready_batch(
    schema_ptr: *mut ArrowSchema,
    array_ptr: *mut ArrowArray,
    expected_schema: Option<&arrow::datatypes::Schema>,
) -> Result<MicroPartitionRef, String> {
    unsafe {
        validate_schema(schema_ptr)?;
        validate_array(array_ptr)?;
    }

    let schema = unsafe { &*schema_ptr };
    let arrow_schema = arrow::datatypes::Schema::try_from(schema)
        .map_err(|error| format!("invalid record-batch schema: {error}"))?;

    let array = unsafe { &*array_ptr };
    // Import through a non-owning view.  Its no-op release means every
    // conversion failure leaves producer callbacks untouched and caller-owned.
    let borrowed_array = FFI_ArrowArray {
        length: array.length,
        null_count: array.null_count,
        offset: array.offset,
        n_buffers: array.n_buffers,
        n_children: array.n_children,
        buffers: array.buffers,
        children: array.children,
        dictionary: array.dictionary,
        release: Some(release_borrowed_view),
        private_data: ptr::null_mut(),
    };

    let data = match unsafe { from_ffi(borrowed_array, schema) } {
        Ok(data) => data,
        Err(error) => return Err(format!("invalid Arrow array: {error}")),
    };
    let array = make_array(data);
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "top-level Arrow value must be a struct RecordBatch".to_string())?;
    if struct_array.num_columns() != arrow_schema.fields().len() {
        return Err("Arrow schema and array child counts differ".to_string());
    }

    // PostgreSQL TupleDesc exposes catalog column names, while PgDaftPlan
    // deliberately uses stable internal names.  The execution entrypoint has
    // already compared every field's type and nullability to expected_schema;
    // use those IR names for the Daft MicroPartition so physical expressions
    // bind to the planned columns without weakening type validation.
    let logical_schema = expected_schema.unwrap_or(&arrow_schema);
    let daft_schema: Schema = logical_schema
        .try_into()
        .map_err(|error| format!("Arrow schema is unsupported by Daft: {error}"))?;
    // P0 deliberately deep-copies every child.  This guarantees that the
    // returned MicroPartition contains no borrowed PostgreSQL memory, including
    // for empty batches whose Arrow arrays may have no owner-carrying buffers.
    let owned_columns = struct_array
        .columns()
        .iter()
        .map(|column| {
            let source = column.to_data();
            let mut owned = MutableArrayData::new(vec![&source], false, source.len());
            owned.extend(0, 0, source.len());
            make_array(owned.freeze())
        })
        .collect();
    let micropartition = MicroPartition::from_arrow(Arc::new(daft_schema), owned_columns)
        .map_err(|error| format!("cannot build Daft MicroPartition: {error}"))?;
    let micropartition = Arc::new(micropartition);

    // All fallible work and all external-buffer reads are complete.  Accept
    // ownership, release the producer resources exactly once, and make the
    // successful transfer visible to the caller by clearing both callbacks.
    unsafe {
        if let Some(release) = (*array_ptr).release {
            release(array_ptr);
        }
        (*array_ptr).release = None;
        if let Some(release) = (*schema_ptr).release {
            release(schema_ptr);
        }
        (*schema_ptr).release = None;
    }

    Ok(micropartition)
}

/// Try to import a C Data RecordBatch after bounded-queue admission.
///
/// # Safety
///
/// `schema` and `array` must point to live Arrow C Data structs for the whole
/// call.  On `Imported`, their release fields are cleared and the caller must
/// not use their other fields.  On `Again` or `Error`, both structs are
/// unchanged and remain caller-owned.
pub unsafe fn try_import_arrow_batch(
    schema: *mut ArrowSchema,
    array: *mut ArrowArray,
    admission: ArrowImportAdmission,
) -> ArrowBatchImport {
    if admission == ArrowImportAdmission::Again {
        return ArrowBatchImport::Again;
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        import_ready_batch(schema, array, None)
    })) {
        Ok(Ok(batch)) => ArrowBatchImport::Imported(batch),
        Ok(Err(message)) => ArrowBatchImport::Error(message),
        Err(_) => ArrowBatchImport::Error("panic while importing Arrow C Data".to_string()),
    }
}

/// Import a validated PostgreSQL batch while assigning the stable source
/// field names carried by PgDaftPlan.
///
/// # Safety
///
/// The caller must first verify that every actual field has the same data type
/// and nullability as `expected_schema`. Ownership follows
/// [`try_import_arrow_batch`].
pub unsafe fn try_import_arrow_batch_as(
    schema: *mut ArrowSchema,
    array: *mut ArrowArray,
    admission: ArrowImportAdmission,
    expected_schema: &arrow::datatypes::Schema,
) -> ArrowBatchImport {
    if admission == ArrowImportAdmission::Again {
        return ArrowBatchImport::Again;
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        import_ready_batch(schema, array, Some(expected_schema))
    })) {
        Ok(Ok(batch)) => ArrowBatchImport::Imported(batch),
        Ok(Err(message)) => ArrowBatchImport::Error(message),
        Err(_) => ArrowBatchImport::Error("panic while importing Arrow C Data".to_string()),
    }
}

/// Low-level C test entrypoint.  Production execution uses
/// `try_import_arrow_batch` and passes the returned MicroPartition to its
/// bounded source queue.
///
/// # Safety
///
/// Follows the ownership rules documented on `try_import_arrow_batch`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_import_arrow_batch(
    schema: *mut ArrowSchema,
    array: *mut ArrowArray,
    error_out: *mut *mut DpgError,
) -> ArrowImportResult {
    match unsafe { try_import_arrow_batch(schema, array, ArrowImportAdmission::Ready) } {
        ArrowBatchImport::Imported(batch) => {
            drop(batch);
            ArrowImportResult::Ok
        }
        ArrowBatchImport::Again => ArrowImportResult::Again,
        ArrowBatchImport::Error(message) => {
            if !error_out.is_null() {
                unsafe {
                    *error_out = Box::into_raw(DpgError::new(DpgErrorCode::InvalidArg, message));
                }
            }
            ArrowImportResult::Error
        }
    }
}
