//! Deterministic P2 IR/Arrow/ABI fuzz corpus and feature-gated failpoints.

#![cfg(feature = "failure-injection")]

use std::{
    ffi::{CStr, CString},
    ptr,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StructArray},
    datatypes::{DataType, Field},
    ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use daft_pg_bridge::{
    arrow::{try_import_arrow_batch, ArrowBatchImport, ArrowImportAdmission},
    capabilities::CAPABILITY_DIGEST,
    error::{dpg_error_code, DpgError, DpgErrorCode},
    execution::{
        dpg_cancel_v1, dpg_close_v1, dpg_error_free_v1, dpg_error_message_v1,
        dpg_metrics_snapshot_v1, dpg_open_v1, dpg_try_next_batch_v1, dpg_try_push_batch_v1,
        dpg_wait_v1, DpgConfigV1, DpgHandle, DpgMetricsV1, DpgStatus,
    },
    fault_injection::{arm, clear, FaultPoint},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn config() -> DpgConfigV1 {
    DpgConfigV1 {
        struct_size: std::mem::size_of::<DpgConfigV1>() as u32,
        worker_threads: 1,
        max_inflight_batches_per_source: 1,
        memory_limit_bytes: 1024 * 1024,
        spill_dir: ptr::null(),
        wait_timeout_ms: 1,
    }
}

fn source_only_plan() -> Vec<u8> {
    let field = json!({
        "column_id": 1,
        "name": "_pgd_f77_s0_c1",
        "type": {
            "pg_oid": 20,
            "typmod": -1,
            "collation_oid": 0,
            "nullable": true,
            "arrow_format": "l",
            "daft_dtype": "Int64"
        }
    });
    let mut value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": 15,
        "fragment_id": 77,
        "root_node_id": 1,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [field.clone()]}],
        "nodes": [{"id": 1, "kind": "source", "source_id": 0, "output": [field.clone()]}],
        "output_schema": [field]
    });
    let canonical = serde_json::to_vec(&value).expect("serialize source-only plan");
    value.as_object_mut().unwrap().insert(
        "payload_sha256".to_string(),
        Value::String(format!("{:x}", Sha256::digest(canonical))),
    );
    serde_json::to_vec(&value).expect("serialize stamped source-only plan")
}

unsafe fn take_error(error: &mut *mut DpgError) -> (DpgErrorCode, String) {
    assert!(!(*error).is_null(), "Bridge error object is required");
    let code = unsafe { dpg_error_code(*error) };
    let message_ptr = unsafe { dpg_error_message_v1(*error) };
    assert!(!message_ptr.is_null(), "Bridge error message is required");
    let message = unsafe { CStr::from_ptr(message_ptr) }
        .to_string_lossy()
        .into_owned();
    unsafe { dpg_error_free_v1(error) };
    assert!((*error).is_null());
    (code, message)
}

unsafe fn open_bytes(bytes: &[u8]) -> (DpgStatus, *mut DpgHandle, *mut DpgError) {
    let mut handle = ptr::null_mut();
    let mut error = ptr::null_mut();
    let status = unsafe {
        dpg_open_v1(
            bytes.as_ptr(),
            bytes.len() as u64,
            &config(),
            &mut handle,
            &mut error,
        )
    };
    (status, handle, error)
}

unsafe fn close_twice(handle: &mut *mut DpgHandle) {
    if !(*handle).is_null() {
        unsafe {
            dpg_cancel_v1(*handle);
            dpg_cancel_v1(*handle);
        }
    }
    unsafe {
        dpg_close_v1(handle);
        dpg_close_v1(handle);
    }
    assert!((*handle).is_null());
}

fn assert_redacted(message: &str) {
    let lower = message.to_ascii_lowercase();
    for forbidden in [
        "/home/",
        "/users/",
        "/tmp/pgdaft-",
        "password=",
        "token=",
        "secret=",
        "authorization:",
        "bearer ",
        "postgres://",
        "http://",
        "https://",
    ] {
        assert!(!lower.contains(forbidden), "sensitive error class leaked");
    }
}

#[test]
fn injected_open_panic_and_oom_are_contained_then_recover() {
    clear();
    arm(FaultPoint::OpenPanic);
    let (status, mut handle, mut error) = unsafe { open_bytes(b"{}") };
    assert_eq!(status, DpgStatus::Error);
    assert!(handle.is_null());
    let (code, message) = unsafe { take_error(&mut error) };
    assert_eq!(code, DpgErrorCode::InternalError);
    assert_eq!(
        message,
        "PGDAFT_PANIC: internal panic contained at ABI boundary"
    );

    arm(FaultPoint::OpenResourceExhausted);
    let (status, next_handle, mut error) = unsafe { open_bytes(b"{}") };
    handle = next_handle;
    assert_eq!(status, DpgStatus::Error);
    assert!(handle.is_null());
    let (code, message) = unsafe { take_error(&mut error) };
    assert_eq!(code, DpgErrorCode::ResourceExhausted);
    assert_eq!(message, "PGDAFT_RESOURCE_EXHAUSTED_INJECTED");

    let plan = source_only_plan();
    let (status, next_handle, error) = unsafe { open_bytes(&plan) };
    handle = next_handle;
    assert_eq!(status, DpgStatus::Ok);
    assert!(!handle.is_null());
    assert!(error.is_null());
    unsafe { close_twice(&mut handle) };
    clear();
}

#[test]
fn injected_corruption_and_poll_panic_remain_cancellable() {
    clear();
    let plan = source_only_plan();
    let (status, mut handle, error) = unsafe { open_bytes(&plan) };
    assert_eq!(status, DpgStatus::Ok);
    assert!(error.is_null());

    arm(FaultPoint::PushCorruption);
    let mut error = ptr::null_mut();
    let status =
        unsafe { dpg_try_push_batch_v1(handle, 0, ptr::null_mut(), ptr::null_mut(), &mut error) };
    assert_eq!(status, DpgStatus::Error);
    let (code, message) = unsafe { take_error(&mut error) };
    assert_eq!(code, DpgErrorCode::InvalidArg);
    assert_eq!(message, "PGDAFT_ARROW_CORRUPTION_INJECTED");
    unsafe { close_twice(&mut handle) };

    let (status, mut handle, error) = unsafe { open_bytes(&plan) };
    assert_eq!(status, DpgStatus::Ok);
    assert!(error.is_null());
    arm(FaultPoint::NextPanic);
    let mut output_array = FFI_ArrowArray::empty();
    let mut output_schema = FFI_ArrowSchema::empty();
    let mut error = ptr::null_mut();
    assert_eq!(
        unsafe {
            dpg_try_next_batch_v1(handle, &mut output_array, &mut output_schema, &mut error)
        },
        DpgStatus::Error
    );
    let (_, message) = unsafe { take_error(&mut error) };
    assert_eq!(
        message,
        "PGDAFT_PANIC: internal panic contained at ABI boundary"
    );
    unsafe { close_twice(&mut handle) };

    let (status, mut handle, error) = unsafe { open_bytes(&plan) };
    assert_eq!(status, DpgStatus::Ok, "post-fault open must recover");
    assert!(error.is_null());
    unsafe { close_twice(&mut handle) };
    clear();
}

#[test]
fn ir_corpus_and_deterministic_byte_mutations_never_open_or_panic() {
    let mut corpus = vec![
        include_bytes!("../fixtures/p2_faults/empty_object.json").to_vec(),
        include_bytes!("../fixtures/p2_faults/truncated_object.json").to_vec(),
        include_bytes!("../fixtures/p2_faults/unknown_top_level.json").to_vec(),
        include_bytes!("../fixtures/p2_faults/wrong_scalar_types.json").to_vec(),
        include_bytes!("../fixtures/p2_faults/deep_nesting.json").to_vec(),
    ];
    corpus.push(
        hex::decode(
            std::str::from_utf8(include_bytes!("../fixtures/p2_faults/invalid_utf8.hex"))
                .unwrap()
                .trim(),
        )
        .unwrap(),
    );

    let seed = include_bytes!("../fixtures/p2_faults/wrong_scalar_types.json");
    for index in 0..seed.len().min(128) {
        let mut mutated = seed.to_vec();
        mutated[index] ^= 0x5a;
        corpus.push(mutated);
    }
    corpus.push(vec![b' '; 1024 * 1024 + 1]);

    for bytes in corpus {
        let (status, mut handle, mut error) = unsafe { open_bytes(&bytes) };
        assert_ne!(status, DpgStatus::Ok);
        assert!(handle.is_null());
        let (_, message) = unsafe { take_error(&mut error) };
        assert_redacted(&message);
        unsafe { close_twice(&mut handle) };
    }
}

fn arrow_batch() -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let field = Arc::new(Field::new("value", DataType::Int64, true));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
    let batch = StructArray::from(vec![(field, values)]);
    to_ffi(&batch.to_data()).expect("export Arrow C Data")
}

#[test]
fn readable_arrow_metadata_corruption_preserves_ownership() {
    for case in 0..6 {
        let (mut array, mut schema) = arrow_batch();
        let saved_length = array.length;
        let saved_null_count = array.null_count;
        let saved_offset = array.offset;
        let saved_buffers = array.buffers;
        let saved_children = array.children;
        let saved_schema_children = schema.children;
        let saved_format = schema.format;
        let invalid_utf8 = CString::new(vec![0xff]).unwrap();
        match case {
            0 => array.length = -1,
            1 => array.null_count = -2,
            2 => array.offset = -1,
            3 => array.buffers = ptr::null_mut(),
            4 => array.children = ptr::null_mut(),
            5 => schema.format = invalid_utf8.as_ptr(),
            _ => unreachable!(),
        }
        let result =
            unsafe { try_import_arrow_batch(&mut schema, &mut array, ArrowImportAdmission::Ready) };
        let ArrowBatchImport::Error(message) = result else {
            panic!("corrupt Arrow metadata must be rejected");
        };
        assert_redacted(&message);
        assert!(
            array.release.is_some(),
            "array ownership must stay with caller"
        );
        assert!(
            schema.release.is_some(),
            "schema ownership must stay with caller"
        );

        // Restore every producer pointer/count before the normal Drop release.
        array.length = saved_length;
        array.null_count = saved_null_count;
        array.offset = saved_offset;
        array.buffers = saved_buffers;
        array.children = saved_children;
        schema.children = saved_schema_children;
        schema.format = saved_format;
    }
}

#[test]
fn abi_null_matrix_and_error_redaction_are_stable() {
    let sensitive = DpgError::new(
        DpgErrorCode::InternalError,
        "password=hunter2 token=abc https://example.invalid /home/alice/private".to_string(),
    );
    assert_eq!(sensitive.message(), "PGDAFT_ERROR_PATH_REDACTED <HOME>");
    assert_redacted(sensitive.message());
    let unicode = DpgError::new(DpgErrorCode::InternalError, "数".repeat(400));
    assert!(unicode.message().contains("(truncated)"));

    let mut error = ptr::null_mut();
    let mut events = u64::MAX;
    assert_eq!(
        unsafe { dpg_wait_v1(ptr::null_mut(), 1, &mut events, &mut error) },
        DpgStatus::Error
    );
    let (_, message) = unsafe { take_error(&mut error) };
    assert_eq!(message, "PGDAFT_HANDLE_NULL");

    let mut metrics = DpgMetricsV1::default();
    metrics.struct_size = std::mem::size_of::<DpgMetricsV1>() as u32;
    assert_eq!(
        unsafe { dpg_metrics_snapshot_v1(ptr::null_mut(), &mut metrics, &mut error) },
        DpgStatus::Error
    );
    let (_, message) = unsafe { take_error(&mut error) };
    assert_eq!(message, "PGDAFT_HANDLE_NULL");

    let plan = source_only_plan();
    let mut handle = ptr::null_mut();
    assert_eq!(
        unsafe {
            dpg_open_v1(
                plan.as_ptr(),
                plan.len() as u64,
                &config(),
                ptr::null_mut(),
                &mut error,
            )
        },
        DpgStatus::Error
    );
    let (_, message) = unsafe { take_error(&mut error) };
    assert_eq!(message, "PGDAFT_OPEN_OUT_NULL");
    unsafe { close_twice(&mut handle) };
}
