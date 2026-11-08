//! Arrow C Data ownership tests using real arrow-rs exported buffers.

use std::{
    ffi::c_void,
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use arrow::{
    array::{Array, ArrayRef, Int32Array, StructArray},
    datatypes::{DataType, Field},
    ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use daft_pg_bridge::{
    arrow::{
        try_import_arrow_batch, ArrowArray, ArrowBatchImport, ArrowImportAdmission, ArrowSchema,
    },
    error::DpgError,
};

struct ArrayTracker {
    release: Option<unsafe extern "C" fn(*mut FFI_ArrowArray)>,
    private_data: *mut c_void,
    count: Arc<AtomicUsize>,
}

struct SchemaTracker {
    release: Option<unsafe extern "C" fn(*mut FFI_ArrowSchema)>,
    private_data: *mut c_void,
    count: Arc<AtomicUsize>,
}

unsafe extern "C" fn tracked_array_release(array: *mut FFI_ArrowArray) {
    let tracker = unsafe { Box::from_raw((*array).private_data.cast::<ArrayTracker>()) };
    unsafe {
        (*array).private_data = tracker.private_data;
        (*array).release = tracker.release;
    }
    tracker.count.fetch_add(1, Ordering::SeqCst);
    if let Some(release) = tracker.release {
        unsafe { release(array) };
    }
}

unsafe extern "C" fn tracked_schema_release(schema: *mut FFI_ArrowSchema) {
    let tracker = unsafe { Box::from_raw((*schema).private_data.cast::<SchemaTracker>()) };
    unsafe {
        (*schema).private_data = tracker.private_data;
        (*schema).release = tracker.release;
    }
    tracker.count.fetch_add(1, Ordering::SeqCst);
    if let Some(release) = tracker.release {
        unsafe { release(schema) };
    }
}

fn tracked_batch() -> (ArrowSchema, ArrowArray, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let field = Arc::new(Field::new("value", DataType::Int32, true));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]));
    let struct_array = StructArray::from(vec![(field, values)]);
    let (mut array, mut schema) = to_ffi(&struct_array.to_data()).expect("export C Data");

    let array_count = Arc::new(AtomicUsize::new(0));
    let schema_count = Arc::new(AtomicUsize::new(0));
    let array_tracker = Box::new(ArrayTracker {
        release: array.release,
        private_data: array.private_data,
        count: array_count.clone(),
    });
    array.private_data = Box::into_raw(array_tracker).cast::<c_void>();
    array.release = Some(tracked_array_release);
    let schema_tracker = Box::new(SchemaTracker {
        release: schema.release,
        private_data: schema.private_data,
        count: schema_count.clone(),
    });
    schema.private_data = Box::into_raw(schema_tracker).cast::<c_void>();
    schema.release = Some(tracked_schema_release);

    (schema, array, schema_count, array_count)
}

unsafe fn caller_release(schema: &mut ArrowSchema, array: &mut ArrowArray) {
    if let Some(release) = array.release {
        unsafe { release(array) };
    }
    if let Some(release) = schema.release {
        unsafe { release(schema) };
    }
}

#[test]
fn ok_transfers_real_buffers_and_releases_exactly_once() {
    let (mut schema, mut array, schema_count, array_count) = tracked_batch();
    let batch = match unsafe {
        try_import_arrow_batch(&mut schema, &mut array, ArrowImportAdmission::Ready)
    } {
        ArrowBatchImport::Imported(batch) => batch,
        other => panic!("real Arrow batch must import, got {other:?}"),
    };

    assert!(schema.release.is_none());
    assert!(array.release.is_none());
    assert_eq!(batch.len(), 3);
    assert_eq!(batch.column_names(), vec!["value"]);
    assert_eq!(schema_count.load(Ordering::SeqCst), 1);
    assert_eq!(array_count.load(Ordering::SeqCst), 1);
    let values = batch.record_batches()[0]
        .get_column(0)
        .to_arrow()
        .expect("Daft column remains readable after producer release");
    let values = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 result");
    assert_eq!(values.value(0), 10);
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 30);

    let second_reference = batch.clone();
    drop(batch);
    drop(second_reference);
    assert_eq!(array_count.load(Ordering::SeqCst), 1);
    assert_eq!(schema_count.load(Ordering::SeqCst), 1);
}

#[test]
fn again_preserves_caller_ownership() {
    let (mut schema, mut array, schema_count, array_count) = tracked_batch();
    assert!(matches!(
        unsafe { try_import_arrow_batch(&mut schema, &mut array, ArrowImportAdmission::Again) },
        ArrowBatchImport::Again
    ));
    assert!(schema.release.is_some());
    assert!(array.release.is_some());
    assert_eq!(schema_count.load(Ordering::SeqCst), 0);
    assert_eq!(array_count.load(Ordering::SeqCst), 0);

    unsafe { caller_release(&mut schema, &mut array) };
    assert_eq!(array_count.load(Ordering::SeqCst), 1);
    assert_eq!(schema_count.load(Ordering::SeqCst), 1);
}

#[test]
fn error_preserves_caller_ownership() {
    let (mut schema, mut array, schema_count, array_count) = tracked_batch();
    array.length = -1;
    assert!(matches!(
        unsafe { try_import_arrow_batch(&mut schema, &mut array, ArrowImportAdmission::Ready) },
        ArrowBatchImport::Error(_)
    ));
    assert!(schema.release.is_some());
    assert!(array.release.is_some());
    assert_eq!(schema_count.load(Ordering::SeqCst), 0);
    assert_eq!(array_count.load(Ordering::SeqCst), 0);

    array.length = 3;
    unsafe { caller_release(&mut schema, &mut array) };
    assert_eq!(array_count.load(Ordering::SeqCst), 1);
    assert_eq!(schema_count.load(Ordering::SeqCst), 1);
}

#[test]
fn conversion_error_preserves_caller_ownership() {
    let (mut schema, mut array, schema_count, array_count) = tracked_batch();
    // The top-level struct schema requires one child; this mismatch is caught
    // by arrow-rs while the ownership guard is still disarmed.
    array.n_children = 0;
    array.children = ptr::null_mut();
    assert!(matches!(
        unsafe { try_import_arrow_batch(&mut schema, &mut array, ArrowImportAdmission::Ready) },
        ArrowBatchImport::Error(_)
    ));
    assert!(schema.release.is_some());
    assert!(array.release.is_some());
    assert_eq!(schema_count.load(Ordering::SeqCst), 0);
    assert_eq!(array_count.load(Ordering::SeqCst), 0);

    // Restore the producer view before caller cleanup.
    // Re-exporting is simpler and also releases this malformed pair once.
    unsafe { caller_release(&mut schema, &mut array) };
    assert_eq!(array_count.load(Ordering::SeqCst), 1);
    assert_eq!(schema_count.load(Ordering::SeqCst), 1);
}

#[test]
fn null_arguments_are_error_without_touching_the_other_owner() {
    let (mut schema, mut array, schema_count, array_count) = tracked_batch();
    assert!(matches!(
        unsafe { try_import_arrow_batch(ptr::null_mut(), &mut array, ArrowImportAdmission::Ready) },
        ArrowBatchImport::Error(_)
    ));
    assert!(array.release.is_some());
    assert!(matches!(
        unsafe {
            try_import_arrow_batch(&mut schema, ptr::null_mut(), ArrowImportAdmission::Ready)
        },
        ArrowBatchImport::Error(_)
    ));
    assert!(schema.release.is_some());

    unsafe { caller_release(&mut schema, &mut array) };
    assert_eq!(array_count.load(Ordering::SeqCst), 1);
    assert_eq!(schema_count.load(Ordering::SeqCst), 1);
}

#[test]
fn c_entrypoint_reports_error_without_consuming() {
    let (mut schema, mut array, schema_count, array_count) = tracked_batch();
    array.offset = -1;
    let mut error: *mut DpgError = ptr::null_mut();
    let result = unsafe {
        daft_pg_bridge::arrow::dpg_import_arrow_batch(&mut schema, &mut array, &mut error)
    };
    assert_eq!(result, daft_pg_bridge::arrow::ArrowImportResult::Error);
    assert!(!error.is_null());
    assert!(schema.release.is_some());
    assert!(array.release.is_some());
    unsafe {
        daft_pg_bridge::error::dpg_error_free(&mut error);
        array.offset = 0;
        caller_release(&mut schema, &mut array);
    }
    assert_eq!(array_count.load(Ordering::SeqCst), 1);
    assert_eq!(schema_count.load(Ordering::SeqCst), 1);
}
