//! Frozen C ABI v1 and the opaque PostgreSQL/Daft execution handle.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::{c_char, CStr},
    ptr, slice,
    sync::Arc,
    time::Duration,
};

use arrow::{
    array::{new_empty_array, Array, ArrayRef, Int8Array, StructArray},
    datatypes::{DataType, Field as ArrowField, Fields, Schema as ArrowSchemaType, TimeUnit},
    ffi::to_ffi,
};
use daft_local_execution::prepared_execution::{
    BuildPushResult, FinishResult, NextResult, PreparedExecutionConfig, PreparedNativeExecution,
    WaitResult,
};
use serde::{de, Deserialize, Deserializer};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

use crate::{
    abi::{SpillDirectory, SPILL_CLEANED, SPILL_CLEANUP_FAILED, SPILL_PENDING},
    arrow::{
        try_import_arrow_batch_as, ArrowArray, ArrowBatchImport, ArrowImportAdmission, ArrowSchema,
    },
    capabilities::{RuntimeCapability, ABI_MAJOR, ABI_MINOR, CAPABILITY_DIGEST},
    error::{dpg_error_free, dpg_error_message, DpgError, DpgErrorCode},
    ir::{PgDaftPlanV1, SourceDescriptor},
    plan::translate_plan,
};

const MAX_PLAN_BYTES: usize = 1024 * 1024;
const MAX_WORKER_THREADS: usize = 256;
const MAX_QUEUE_DEPTH: usize = 4096;
// Frozen PG Arrow v1 accepts at most 65536 rows in one output batch. Daft
// operators (notably a duplicate-heavy HashJoin) may emit a larger
// MicroPartition, so the Bridge must slice it before crossing the ABI.
const MAX_OUTPUT_BATCH_ROWS: usize = 65536;
// P0 bits 0..5 plus the admitted P1 Filter, HashJoin, HashAggregate, Sort and
// TopN entry points. A bit is exposed only after its end-to-end gate is enabled.
const ENABLED_RUNTIME_CAPABILITIES: u64 = RuntimeCapability::PLAN_V1 as u64
    | RuntimeCapability::ARROW_C_DATA_V1 as u64
    | RuntimeCapability::EMBEDDED_SWORDFISH as u64
    | RuntimeCapability::PG_OWNED_SOURCE as u64
    | RuntimeCapability::PROJECT as u64
    | RuntimeCapability::LIMIT as u64
    | RuntimeCapability::FILTER as u64
    | RuntimeCapability::HASH_JOIN as u64
    | RuntimeCapability::HASH_AGGREGATE as u64
    | RuntimeCapability::SORT as u64
    | RuntimeCapability::TOP_N as u64;
const ARROW_SCHEMA_CONTRACT_VERSION: u32 = 1;
const ARROW_SCHEMA_FINGERPRINT_DOMAIN: &[u8] = b"PGDAFT_ARROW_SCHEMA_V1";
const ARROW_SCHEMA_FINGERPRINT_KEY: &str = "pg_daft_schema_fingerprint_v1";

/// `dpg_wait_v1` observed input or output progress.
pub const DPG_EVENT_PROGRESS: u64 = 1 << 0;
/// An output batch can be retrieved without waiting.
pub const DPG_EVENT_OUTPUT_READY: u64 = 1 << 1;
/// Execution reached normal end-of-stream.
pub const DPG_EVENT_END: u64 = 1 << 2;
/// Execution was cancelled or failed.
pub const DPG_EVENT_ERROR: u64 = 1 << 3;

/// Spill is not enabled for this handle.
pub const DPG_SPILL_NOT_ENABLED: u32 = 0;
/// A configured per-handle spill directory is live.
pub const DPG_SPILL_PENDING: u32 = SPILL_PENDING;
/// The per-handle spill directory was removed successfully.
pub const DPG_SPILL_CLEANED: u32 = SPILL_CLEANED;
/// Cleanup was attempted but the per-handle directory remains.
pub const DPG_SPILL_CLEANUP_FAILED: u32 = SPILL_CLEANUP_FAILED;

/// Stable status values in `daft_pg_bridge.h`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpgStatus {
    /// Operation completed and any documented ownership transfer occurred.
    Ok = 0,
    /// No bounded capacity/output/event is currently available.
    Again = 1,
    /// Permanent normal end-of-stream.
    End = 2,
    /// Invalid input, invalid state, cancellation, or execution failure.
    Error = 3,
    /// ABI, capability registry, or fixed runtime configuration mismatch.
    Incompatible = 4,
}

/// Execution configuration copied by `dpg_open_v1`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DpgConfigV1 {
    /// Size of the caller's struct for ABI extension safety.
    pub struct_size: u32,
    /// Daft compute worker count, fixed on first open in this backend.
    pub worker_threads: u32,
    /// Bounded queue depth for each source and the fragment output.
    pub max_inflight_batches_per_source: u32,
    /// Execution-local memory budget.
    pub memory_limit_bytes: u64,
    /// Optional NUL-terminated spill directory; copied during open.
    pub spill_dir: *const c_char,
    /// Maximum wait duration accepted by `dpg_wait_v1`.
    pub wait_timeout_ms: u32,
}

/// Size-versioned, data-free metrics snapshot for ABI 1.1.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DpgMetricsV1 {
    /// Caller-provided size; must cover this v1 prefix.
    pub struct_size: u32,
    /// Must be zero.
    pub reserved0: u32,
    /// Rows accepted from all PG-owned sources.
    pub input_rows: u64,
    /// Non-empty batches accepted from all PG-owned sources.
    pub input_batches: u64,
    /// Logical Arrow payload bytes accepted from all sources.
    pub input_bytes: u64,
    /// Rows transferred to PostgreSQL.
    pub output_rows: u64,
    /// Non-empty batches transferred to PostgreSQL.
    pub output_batches: u64,
    /// Logical Arrow payload bytes transferred to PostgreSQL.
    pub output_bytes: u64,
    /// Sum of active durations across Daft nodes; not additive with wall time.
    pub operator_compute_us: u64,
    /// Current execution-local reserved bytes, not RSS.
    pub current_memory_bytes: u64,
    /// Peak execution-local reserved bytes, not RSS.
    pub peak_memory_bytes: u64,
    /// Bytes spilled by this handle.
    pub spill_bytes: u64,
    /// Spill operations performed by this handle.
    pub spill_count: u64,
    /// One of `dpg_spill_cleanup_status_v1`.
    pub spill_cleanup_status: u32,
    /// Must be zero.
    pub reserved1: u32,
}

/// Opaque handle owned by the PostgreSQL backend.
#[repr(C)]
pub struct DpgHandle {
    execution: PreparedNativeExecution,
    spill_directory: Option<SpillDirectory>,
    source_schemas: HashMap<u32, ArrowSchemaType>,
    output_schema: ArrowSchemaType,
    output_schema_fingerprint: String,
    max_wait_ms: u32,
    pending_output: VecDeque<daft_micropartition::MicroPartitionRef>,
    input_rows: u64,
    input_batches: u64,
    input_bytes: u64,
    output_rows: u64,
    output_batches: u64,
    output_bytes: u64,
}

impl DpgHandle {
    fn cleanup_terminal_spill(&self) {
        if let Some(directory) = &self.spill_directory {
            directory.cleanup();
        }
    }

    fn abort_and_cleanup(&mut self) {
        self.execution.cancel_and_wait();
        self.pending_output.clear();
        self.cleanup_terminal_spill();
    }
}

impl Drop for DpgHandle {
    fn drop(&mut self) {
        self.abort_and_cleanup();
    }
}

/// Return the supported ABI major without initializing a runtime.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_abi_major() -> u32 {
    std::panic::catch_unwind(|| ABI_MAJOR).unwrap_or(0)
}

/// Return the supported ABI minor without initializing a runtime.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_abi_minor() -> u32 {
    std::panic::catch_unwind(|| ABI_MINOR).unwrap_or(0)
}

/// Return currently executable runtime prerequisites and entry points.
///
/// This is a pure query: it allocates no query state and starts no threads.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_capabilities() -> u64 {
    std::panic::catch_unwind(|| ENABLED_RUNTIME_CAPABILITIES).unwrap_or(0)
}

/// Copy the binary SHA-256 capability-registry digest to caller memory.
///
/// # Safety
/// `out_sha256` must point to at least 32 writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_capability_digest_v1(out_sha256: *mut u8) -> DpgStatus {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if out_sha256.is_null() {
            return DpgStatus::Error;
        }
        let Some(bytes) = decode_digest(CAPABILITY_DIGEST) else {
            return DpgStatus::Error;
        };
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), out_sha256, bytes.len()) };
        DpgStatus::Ok
    })) {
        Ok(status) => status,
        Err(_) => DpgStatus::Error,
    }
}

/// Parse, validate, lower, and start a `PgDaftPlan` v1 fragment.
///
/// # Safety
/// `plan` must reference `plan_len` readable bytes, `config` must reference a
/// complete v1 prefix, and `out` must reference a NULL handle slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_open_v1(
    plan: *const u8,
    plan_len: u64,
    config: *const DpgConfigV1,
    out: *mut *mut DpgHandle,
    error_out: *mut *mut DpgError,
) -> DpgStatus {
    ffi_status(error_out, || unsafe {
        if out.is_null() {
            return fail(error_out, DpgStatus::Error, "PGDAFT_OPEN_OUT_NULL");
        }
        if !(*out).is_null() {
            return fail(error_out, DpgStatus::Error, "PGDAFT_OPEN_OUT_NOT_EMPTY");
        }
        if plan.is_null() || plan_len == 0 || plan_len > MAX_PLAN_BYTES as u64 {
            return fail(error_out, DpgStatus::Error, "PGDAFT_PLAN_SIZE_INVALID");
        }
        if config.is_null() {
            return fail(error_out, DpgStatus::Error, "PGDAFT_CONFIG_NULL");
        }

        #[cfg(feature = "failure-injection")]
        {
            use crate::fault_injection::{take, FaultPoint};
            if take(FaultPoint::OpenPanic) {
                panic!("PGDAFT_INJECTED_OPEN_PANIC");
            }
            if take(FaultPoint::OpenResourceExhausted) {
                return fail_with_code(
                    error_out,
                    DpgStatus::Error,
                    DpgErrorCode::ResourceExhausted,
                    "PGDAFT_RESOURCE_EXHAUSTED_INJECTED",
                );
            }
        }

        let config = *config;
        let spill_base = match validate_and_copy_config(&config) {
            Ok(spill_base) => spill_base,
            Err(message) => return fail(error_out, DpgStatus::Error, &message),
        };
        let plan_bytes = slice::from_raw_parts(plan, plan_len as usize);
        let ir = match parse_and_verify_plan(plan_bytes) {
            Ok(ir) => ir,
            Err(message) => {
                let status = if message.contains("CAPABILITY") {
                    DpgStatus::Incompatible
                } else {
                    DpgStatus::Error
                };
                return fail(error_out, status, &message);
            }
        };
        let source_schemas = match expected_source_schemas(&ir.sources) {
            Ok(schemas) => schemas,
            Err(message) => return fail(error_out, DpgStatus::Error, &message),
        };
        let output_schema_fingerprint = match schema_fingerprint(&ir.output_schema) {
            Ok(fingerprint) => fingerprint,
            Err(message) => return fail(error_out, DpgStatus::Error, &message),
        };
        let output_schema = match expected_arrow_schema(&ir.output_schema) {
            Ok(schema) => schema,
            Err(message) => return fail(error_out, DpgStatus::Error, &message),
        };
        let local_plan = match translate_plan(&ir) {
            Ok(plan) => plan,
            Err(error) => {
                let status = if error.message().contains("CAPABILITY") {
                    DpgStatus::Incompatible
                } else {
                    DpgStatus::Error
                };
                return store_error(error_out, status, error);
            }
        };
        let spill_directory = match spill_base {
            Some(base) => match SpillDirectory::create(&base) {
                Ok(directory) => Some(directory),
                Err(message) => return fail(error_out, DpgStatus::Error, &message),
            },
            None => None,
        };
        let prepared_config = PreparedExecutionConfig {
            worker_threads: config.worker_threads as usize,
            queue_depth: config.max_inflight_batches_per_source as usize,
            memory_limit_bytes: config.memory_limit_bytes,
            spill_dir: spill_directory
                .as_ref()
                .map(|directory| directory.path().to_path_buf()),
            maintain_order: true,
        };
        let execution = match PreparedNativeExecution::try_prepare(local_plan, prepared_config) {
            Ok(execution) => execution,
            Err(error) => {
                return fail(
                    error_out,
                    DpgStatus::Incompatible,
                    &format!("PGDAFT_RUNTIME_INIT: {error}"),
                );
            }
        };

        *out = Box::into_raw(Box::new(DpgHandle {
            execution,
            spill_directory,
            source_schemas,
            output_schema,
            output_schema_fingerprint,
            max_wait_ms: config.wait_timeout_ms.max(1),
            pending_output: VecDeque::new(),
            input_rows: 0,
            input_batches: 0,
            input_bytes: 0,
            output_rows: 0,
            output_batches: 0,
            output_bytes: 0,
        }));
        DpgStatus::Ok
    })
}

/// Try to transfer one Arrow C Data RecordBatch to a source.
///
/// On `Ok` the bridge clears both release callbacks and owns the batch.  On
/// every other status the caller retains both C Data objects unchanged.
///
/// # Safety
/// All pointers must satisfy `daft_pg_bridge.h` for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_try_push_batch_v1(
    handle: *mut DpgHandle,
    source_id: u32,
    array: *mut ArrowArray,
    schema: *mut ArrowSchema,
    error_out: *mut *mut DpgError,
) -> DpgStatus {
    ffi_status(error_out, || unsafe {
        let Some(handle) = handle.as_mut() else {
            return fail(error_out, DpgStatus::Error, "PGDAFT_HANDLE_NULL");
        };
        #[cfg(feature = "failure-injection")]
        if crate::fault_injection::take(crate::fault_injection::FaultPoint::PushCorruption) {
            handle.abort_and_cleanup();
            return fail_with_code(
                error_out,
                DpgStatus::Error,
                DpgErrorCode::InvalidArg,
                "PGDAFT_ARROW_CORRUPTION_INJECTED",
            );
        }
        let Some(expected) = handle.source_schemas.get(&source_id).cloned() else {
            handle.abort_and_cleanup();
            return fail(error_out, DpgStatus::Error, "PGDAFT_SOURCE_ID_INVALID");
        };
        if let Err(message) = validate_input_schema(schema, &expected) {
            handle.abort_and_cleanup();
            return fail(error_out, DpgStatus::Error, &message);
        }

        let mut accepted_rows = 0;
        let mut accepted_bytes = 0;
        match handle.execution.try_push_batch_with(source_id, || {
            accepted_bytes = input_boundary_logical_bytes(array, &expected)?;
            match try_import_arrow_batch_as(schema, array, ArrowImportAdmission::Ready, &expected) {
                ArrowBatchImport::Imported(batch) => {
                    accepted_rows = batch.len() as u64;
                    Ok(batch)
                }
                ArrowBatchImport::Again => Err("PGDAFT_INTERNAL_ADMISSION_RACE".to_string()),
                ArrowBatchImport::Error(message)
                    if expected.fields().is_empty()
                        && message.ends_with(
                            "Cannot call RecordBatch::from_arrow() with no arrow arrays.",
                        ) =>
                {
                    let rows = usize::try_from((*array).length)
                        .map_err(|_| "PGDAFT_ARROW_ARRAY_LENGTH_INVALID".to_string())?;
                    let batch = zero_field_micropartition(rows)?;
                    release_accepted_arrow_batch(array, schema);
                    accepted_rows = rows as u64;
                    Ok(batch)
                }
                ArrowBatchImport::Error(message) => Err(format!("PGDAFT_ARROW_IMPORT: {message}")),
            }
        }) {
            BuildPushResult::Accepted => {
                handle.input_rows += accepted_rows;
                handle.input_batches += u64::from(accepted_rows > 0);
                handle.input_bytes += accepted_bytes;
                DpgStatus::Ok
            }
            BuildPushResult::Again => DpgStatus::Again,
            BuildPushResult::End => DpgStatus::End,
            BuildPushResult::InvalidSource => {
                handle.abort_and_cleanup();
                fail(error_out, DpgStatus::Error, "PGDAFT_SOURCE_ID_INVALID")
            }
            BuildPushResult::BuildError(message) => {
                handle.abort_and_cleanup();
                fail(error_out, DpgStatus::Error, &message)
            }
        }
    })
}

/// Try to send this source's one Finish message.
///
/// # Safety
/// `handle` must be a live handle returned by `dpg_open_v1`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_finish_source_v1(
    handle: *mut DpgHandle,
    source_id: u32,
    error_out: *mut *mut DpgError,
) -> DpgStatus {
    ffi_status(error_out, || unsafe {
        let Some(handle) = handle.as_mut() else {
            return fail(error_out, DpgStatus::Error, "PGDAFT_HANDLE_NULL");
        };
        match handle.execution.try_finish_source(source_id) {
            FinishResult::Accepted => DpgStatus::Ok,
            FinishResult::Again => DpgStatus::Again,
            FinishResult::End => DpgStatus::End,
            FinishResult::Error => {
                handle.abort_and_cleanup();
                fail(error_out, DpgStatus::Error, "PGDAFT_SOURCE_ID_INVALID")
            }
        }
    })
}

/// Try to transfer one output RecordBatch to the caller without waiting.
///
/// # Safety
/// Output pointers must reference empty/released Arrow C Data structs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_try_next_batch_v1(
    handle: *mut DpgHandle,
    out_array: *mut ArrowArray,
    out_schema: *mut ArrowSchema,
    error_out: *mut *mut DpgError,
) -> DpgStatus {
    ffi_status(error_out, || unsafe {
        let Some(handle) = handle.as_mut() else {
            return fail(error_out, DpgStatus::Error, "PGDAFT_HANDLE_NULL");
        };
        #[cfg(feature = "failure-injection")]
        if crate::fault_injection::take(crate::fault_injection::FaultPoint::NextPanic) {
            panic!("PGDAFT_INJECTED_NEXT_PANIC");
        }
        if out_array.is_null() || out_schema.is_null() {
            return fail(error_out, DpgStatus::Error, "PGDAFT_OUTPUT_NULL");
        }
        if (*out_array).release.is_some() || (*out_schema).release.is_some() {
            return fail(error_out, DpgStatus::Error, "PGDAFT_OUTPUT_NOT_EMPTY");
        }

        let next = handle
            .pending_output
            .pop_front()
            .map(NextResult::Ready)
            .unwrap_or_else(|| handle.execution.try_next());
        match next {
            NextResult::Ready(batch) => {
                let batch = if batch.len() > MAX_OUTPUT_BATCH_ROWS {
                    let (head, tail) = match batch.split_at(MAX_OUTPUT_BATCH_ROWS) {
                        Ok(parts) => parts,
                        Err(error) => {
                            handle.abort_and_cleanup();
                            return fail(
                                error_out,
                                DpgStatus::Error,
                                &format!("PGDAFT_OUTPUT_SLICE: {error}"),
                            );
                        }
                    };
                    handle.pending_output.push_front(std::sync::Arc::new(tail));
                    std::sync::Arc::new(head)
                } else {
                    batch
                };
                match export_batch(
                    &batch,
                    &handle.output_schema,
                    &handle.output_schema_fingerprint,
                ) {
                    Ok((array, schema)) => {
                        handle.output_rows += batch.len() as u64;
                        handle.output_batches += u64::from(!batch.is_empty());
                        handle.output_bytes += batch.size_bytes() as u64;
                        ptr::write(out_array, array);
                        ptr::write(out_schema, schema);
                        DpgStatus::Ok
                    }
                    Err(message) => {
                        handle.abort_and_cleanup();
                        fail(error_out, DpgStatus::Error, &message)
                    }
                }
            }
            NextResult::NotReady => DpgStatus::Again,
            NextResult::Done => {
                handle.cleanup_terminal_spill();
                DpgStatus::End
            }
            NextResult::Cancelled => {
                handle.abort_and_cleanup();
                fail(error_out, DpgStatus::Error, "PGDAFT_EXECUTION_CANCELLED")
            }
            NextResult::Failed => {
                let message = format!(
                    "PGDAFT_EXECUTION_FAILED: {}",
                    handle
                        .execution
                        .last_error()
                        .unwrap_or_else(|| "unknown pipeline error".to_string())
                );
                handle.abort_and_cleanup();
                fail(error_out, DpgStatus::Error, &message)
            }
        }
    })
}

/// Wait for bounded internal progress without consuming an output batch.
///
/// # Safety
/// `event_bits` must point to writable `u64` storage when non-NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_wait_v1(
    handle: *mut DpgHandle,
    timeout_ms: u32,
    event_bits: *mut u64,
    error_out: *mut *mut DpgError,
) -> DpgStatus {
    ffi_status(error_out, || unsafe {
        let Some(handle) = handle.as_mut() else {
            return fail(error_out, DpgStatus::Error, "PGDAFT_HANDLE_NULL");
        };
        if !event_bits.is_null() {
            *event_bits = 0;
        }
        if !handle.pending_output.is_empty() {
            if !event_bits.is_null() {
                *event_bits = DPG_EVENT_PROGRESS | DPG_EVENT_OUTPUT_READY;
            }
            return DpgStatus::Ok;
        }
        let timeout = timeout_ms.min(handle.max_wait_ms);
        let result = handle
            .execution
            .wait_for_event_blocking(Duration::from_millis(timeout as u64));
        match result {
            WaitResult::Progress => {
                if !event_bits.is_null() {
                    *event_bits = DPG_EVENT_PROGRESS | DPG_EVENT_OUTPUT_READY;
                }
                DpgStatus::Ok
            }
            WaitResult::Timeout => DpgStatus::Again,
            WaitResult::Done => {
                handle.cleanup_terminal_spill();
                if !event_bits.is_null() {
                    *event_bits = DPG_EVENT_END;
                }
                DpgStatus::End
            }
            WaitResult::Cancelled => {
                handle.abort_and_cleanup();
                if !event_bits.is_null() {
                    *event_bits = DPG_EVENT_ERROR;
                }
                fail(error_out, DpgStatus::Error, "PGDAFT_EXECUTION_CANCELLED")
            }
            WaitResult::Failed => {
                let message = format!(
                    "PGDAFT_EXECUTION_FAILED: {}",
                    handle
                        .execution
                        .last_error()
                        .unwrap_or_else(|| "unknown pipeline error".to_string())
                );
                handle.abort_and_cleanup();
                if !event_bits.is_null() {
                    *event_bits = DPG_EVENT_ERROR;
                }
                fail(error_out, DpgStatus::Error, &message)
            }
        }
    })
}

/// Copy a sanitized, read-only execution metrics snapshot.
///
/// # Safety
/// `handle` must be live and `out` must reference writable storage whose
/// caller-initialized `struct_size` covers [`DpgMetricsV1`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_metrics_snapshot_v1(
    handle: *mut DpgHandle,
    out: *mut DpgMetricsV1,
    error_out: *mut *mut DpgError,
) -> DpgStatus {
    ffi_status(error_out, || unsafe {
        let Some(handle) = handle.as_ref() else {
            return fail(error_out, DpgStatus::Error, "PGDAFT_HANDLE_NULL");
        };
        let Some(out_ref) = out.as_ref() else {
            return fail(error_out, DpgStatus::Error, "PGDAFT_METRICS_OUT_NULL");
        };
        if out_ref.struct_size < std::mem::size_of::<DpgMetricsV1>() as u32 {
            return fail(
                error_out,
                DpgStatus::Incompatible,
                "PGDAFT_METRICS_SIZE_INCOMPATIBLE",
            );
        }
        let prepared = match handle.execution.metrics() {
            Ok(metrics) => metrics,
            Err(_) => {
                return fail(
                    error_out,
                    DpgStatus::Error,
                    "PGDAFT_METRICS_SNAPSHOT_FAILED",
                );
            }
        };
        ptr::write(
            out,
            DpgMetricsV1 {
                struct_size: std::mem::size_of::<DpgMetricsV1>() as u32,
                reserved0: 0,
                input_rows: handle.input_rows,
                input_batches: handle.input_batches,
                input_bytes: handle.input_bytes,
                output_rows: handle.output_rows,
                output_batches: handle.output_batches,
                output_bytes: handle.output_bytes,
                operator_compute_us: prepared.operator_compute_us,
                current_memory_bytes: prepared.current_memory_bytes,
                peak_memory_bytes: prepared.peak_memory_bytes,
                spill_bytes: prepared.spill_bytes,
                spill_count: prepared.spill_count,
                spill_cleanup_status: handle
                    .spill_directory
                    .as_ref()
                    .map(SpillDirectory::status)
                    .unwrap_or(DPG_SPILL_NOT_ENABLED),
                reserved1: 0,
            },
        );
        DpgStatus::Ok
    })
}

/// Idempotently cancel a live execution.
///
/// # Safety
/// A non-NULL handle must still be live; a dangling pointer is never valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_cancel_v1(handle: *mut DpgHandle) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if let Some(handle) = handle.as_mut() {
            handle.abort_and_cleanup();
        }
    }));
}

/// Cancel, destroy, and NULL an execution handle.
///
/// # Safety
/// `handle` must be NULL or point to a slot containing NULL/a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_close_v1(handle: *mut *mut DpgHandle) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if handle.is_null() || (*handle).is_null() {
            return;
        }
        let owned = Box::from_raw(*handle);
        *handle = ptr::null_mut();
        drop(owned);
    }));
}

/// Return a borrowed UTF-8 error message.
///
/// # Safety
/// `error` must be NULL or a live bridge error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_error_message_v1(error: *const DpgError) -> *const c_char {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        dpg_error_message(error)
    }))
    .unwrap_or(ptr::null())
}

/// Free and NULL a bridge error object.
///
/// # Safety
/// `error` must be NULL or point to a slot containing NULL/a live error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_error_free_v1(error: *mut *mut DpgError) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        dpg_error_free(error)
    }));
}

fn ffi_status<F>(error_out: *mut *mut DpgError, operation: F) -> DpgStatus
where
    F: FnOnce() -> DpgStatus,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
        Ok(status) => status,
        Err(_) => unsafe {
            fail(
                error_out,
                DpgStatus::Error,
                "PGDAFT_PANIC: internal panic contained at ABI boundary",
            )
        },
    }
}

unsafe fn fail(error_out: *mut *mut DpgError, status: DpgStatus, message: &str) -> DpgStatus {
    unsafe { fail_with_code(error_out, status, DpgErrorCode::InternalError, message) }
}

unsafe fn fail_with_code(
    error_out: *mut *mut DpgError,
    status: DpgStatus,
    code: DpgErrorCode,
    message: &str,
) -> DpgStatus {
    unsafe { store_error(error_out, status, DpgError::new(code, message.to_string())) }
}

unsafe fn store_error(
    error_out: *mut *mut DpgError,
    status: DpgStatus,
    error: Box<DpgError>,
) -> DpgStatus {
    if !error_out.is_null() && unsafe { (*error_out).is_null() } {
        unsafe { *error_out = Box::into_raw(error) };
    }
    status
}

fn validate_and_copy_config(config: &DpgConfigV1) -> Result<Option<String>, String> {
    if config.struct_size < std::mem::size_of::<DpgConfigV1>() as u32 {
        return Err("PGDAFT_CONFIG_SIZE_INCOMPATIBLE".to_string());
    }
    if !(1..=MAX_WORKER_THREADS).contains(&(config.worker_threads as usize)) {
        return Err("PGDAFT_CONFIG_WORKERS_INVALID".to_string());
    }
    if !(1..=MAX_QUEUE_DEPTH).contains(&(config.max_inflight_batches_per_source as usize)) {
        return Err("PGDAFT_CONFIG_QUEUE_DEPTH_INVALID".to_string());
    }
    if config.memory_limit_bytes == 0 {
        return Err("PGDAFT_CONFIG_MEMORY_LIMIT_INVALID".to_string());
    }
    let spill_dir = if config.spill_dir.is_null() {
        None
    } else {
        let spill_dir = unsafe { CStr::from_ptr(config.spill_dir) }
            .to_str()
            .map_err(|_| "PGDAFT_CONFIG_SPILL_DIR_UTF8".to_string())?
            .to_owned();
        if spill_dir.is_empty() {
            return Err("PGDAFT_CONFIG_SPILL_DIR_EMPTY".to_string());
        }
        Some(spill_dir)
    };
    Ok(spill_dir)
}

fn parse_and_verify_plan(bytes: &[u8]) -> Result<PgDaftPlanV1, String> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let UniqueValue(mut value) = UniqueValue::deserialize(&mut deserializer)
        .map_err(|error| format!("PGDAFT_PLAN_JSON: {error}"))?;
    deserializer
        .end()
        .map_err(|error| format!("PGDAFT_PLAN_JSON_TRAILING: {error}"))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "PGDAFT_PLAN_JSON_ROOT: expected object".to_string())?;
    let claimed = object
        .remove("payload_sha256")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| "PGDAFT_PAYLOAD_SHA256_MISSING".to_string())?;
    let canonical =
        serde_json::to_vec(&value).map_err(|error| format!("PGDAFT_PLAN_CANONICALIZE: {error}"))?;
    let actual = format!("{:x}", Sha256::digest(canonical));
    if claimed != actual {
        return Err("PGDAFT_PAYLOAD_SHA256_MISMATCH".to_string());
    }
    value
        .as_object_mut()
        .expect("root was checked above")
        .insert("payload_sha256".to_string(), Value::String(claimed));
    serde_json::from_value(value).map_err(|error| format!("PGDAFT_PLAN_SCHEMA: {error}"))
}

fn expected_source_schemas(
    sources: &[SourceDescriptor],
) -> Result<HashMap<u32, ArrowSchemaType>, String> {
    sources
        .iter()
        .map(|source| Ok((source.source_id, expected_arrow_schema(&source.fields)?)))
        .collect()
}

fn expected_arrow_schema(fields: &[crate::ir::Field]) -> Result<ArrowSchemaType, String> {
    fields
        .iter()
        .map(|field| {
            Ok(ArrowField::new(
                &field.name,
                arrow_data_type(&field.pg_type.arrow_format)?,
                field.pg_type.nullable,
            ))
        })
        .collect::<Result<Vec<_>, String>>()
        .map(ArrowSchemaType::new)
}

fn schema_fingerprint(fields: &[crate::ir::Field]) -> Result<String, String> {
    let nfields = u32::try_from(fields.len())
        .map_err(|_| "PGDAFT_ARROW_SCHEMA_FIELD_COUNT_OVERFLOW".to_string())?;
    let mut hasher = Sha256::new();
    hasher.update(ARROW_SCHEMA_FINGERPRINT_DOMAIN);
    hasher.update(ARROW_SCHEMA_CONTRACT_VERSION.to_be_bytes());
    hasher.update(nfields.to_be_bytes());
    for (index, field) in fields.iter().enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| "PGDAFT_ARROW_SCHEMA_FIELD_INDEX_OVERFLOW".to_string())?;
        let format = field.pg_type.arrow_format.as_bytes();
        let format_len =
            u8::try_from(format.len()).map_err(|_| "PGDAFT_ARROW_FORMAT_TOO_LONG".to_string())?;
        hasher.update(index.to_be_bytes());
        hasher.update(field.pg_type.pg_oid.to_be_bytes());
        hasher.update((field.pg_type.typmod as u32).to_be_bytes());
        hasher.update(field.pg_type.collation_oid.to_be_bytes());
        hasher.update([u8::from(field.pg_type.nullable)]);
        hasher.update([format_len]);
        hasher.update(format);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn arrow_data_type(format: &str) -> Result<DataType, String> {
    match format {
        "b" => Ok(DataType::Boolean),
        "z" => Ok(DataType::Binary),
        "s" => Ok(DataType::Int16),
        "i" => Ok(DataType::Int32),
        "l" => Ok(DataType::Int64),
        "u" => Ok(DataType::Utf8),
        "tdD" => Ok(DataType::Date32),
        "tsu:" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "tsu:UTC" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        other => Err(format!("PGDAFT_ARROW_FORMAT_UNSUPPORTED: {other}")),
    }
}

/// Count the logical payload represented by the caller-owned Arrow C Data
/// buffers before import normalizes them. PostgreSQL deliberately emits one
/// validity bitmap per column even when every value is valid; counting the
/// imported Daft array would lose that buffer and break boundary reconciliation.
unsafe fn input_boundary_logical_bytes(
    array: *const ArrowArray,
    expected: &ArrowSchemaType,
) -> Result<u64, String> {
    let Some(array) = (unsafe { array.as_ref() }) else {
        return Err("PGDAFT_ARROW_ARRAY_NULL".to_string());
    };
    if array.length < 0 || array.offset != 0 {
        return Err("PGDAFT_ARROW_ARRAY_LAYOUT_INVALID".to_string());
    }
    let rows =
        u64::try_from(array.length).map_err(|_| "PGDAFT_ARROW_ARRAY_LENGTH_INVALID".to_string())?;
    let field_count = expected.fields().len();
    if array.n_children != field_count as i64 || (field_count > 0 && array.children.is_null()) {
        return Err("PGDAFT_ARROW_CHILD_COUNT_MISMATCH".to_string());
    }
    let children = if field_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(array.children, field_count) }
    };
    let bitmap_bytes = rows
        .checked_add(7)
        .ok_or_else(|| "PGDAFT_ARROW_LOGICAL_BYTES_OVERFLOW".to_string())?
        / 8;
    let mut total = 0_u64;

    for (child_ptr, field) in children.iter().zip(expected.fields()) {
        let Some(child) = (unsafe { child_ptr.as_ref() }) else {
            return Err("PGDAFT_ARROW_CHILD_NULL".to_string());
        };
        if child.length != array.length || child.offset != 0 || child.n_buffers < 2 {
            return Err("PGDAFT_ARROW_CHILD_LAYOUT_INVALID".to_string());
        }
        let buffer_count = usize::try_from(child.n_buffers)
            .map_err(|_| "PGDAFT_ARROW_BUFFER_COUNT_INVALID".to_string())?;
        if child.buffers.is_null() {
            return Err("PGDAFT_ARROW_BUFFERS_NULL".to_string());
        }
        let buffers = unsafe { slice::from_raw_parts(child.buffers, buffer_count) };
        if !buffers[0].is_null() {
            total = checked_logical_bytes_add(total, bitmap_bytes)?;
        }

        let value_bytes = match field.data_type() {
            DataType::Boolean => bitmap_bytes,
            DataType::Int16 => checked_logical_bytes_mul(rows, 2)?,
            DataType::Int32 | DataType::Date32 => checked_logical_bytes_mul(rows, 4)?,
            DataType::Int64 | DataType::Timestamp(TimeUnit::Microsecond, _) => {
                checked_logical_bytes_mul(rows, 8)?
            }
            DataType::Binary | DataType::Utf8 => {
                if buffers[1].is_null() {
                    return Err("PGDAFT_ARROW_OFFSETS_NULL".to_string());
                }
                let offsets = buffers[1].cast::<i32>();
                let end = unsafe { *offsets.add(rows as usize) };
                if end < 0 {
                    return Err("PGDAFT_ARROW_OFFSETS_INVALID".to_string());
                }
                let offset_bytes = checked_logical_bytes_mul(
                    rows.checked_add(1)
                        .ok_or_else(|| "PGDAFT_ARROW_LOGICAL_BYTES_OVERFLOW".to_string())?,
                    4,
                )?;
                checked_logical_bytes_add(offset_bytes, end as u64)?
            }
            other => return Err(format!("PGDAFT_ARROW_TYPE_UNSUPPORTED: {other}")),
        };
        total = checked_logical_bytes_add(total, value_bytes)?;
    }
    Ok(total)
}

fn zero_field_micropartition(
    rows: usize,
) -> Result<daft_micropartition::MicroPartitionRef, String> {
    // Daft RecordBatch can preserve a non-zero row count with no columns, but
    // its Arrow constructor requires at least one array. Build a private
    // sentinel column and immediately project it away; the resulting source
    // schema remains exactly the zero-field schema frozen in PgDaftPlan.
    let sentinel_schema = ArrowSchemaType::new(vec![ArrowField::new(
        "_pgd_zero_field_row_sentinel",
        DataType::Int8,
        false,
    )]);
    let daft_schema: daft_core::prelude::Schema = (&sentinel_schema)
        .try_into()
        .map_err(|error| format!("PGDAFT_ZERO_FIELD_SCHEMA: {error}"))?;
    let sentinel: ArrayRef = Arc::new(Int8Array::from_iter_values(std::iter::repeat_n(0_i8, rows)));
    let batch =
        daft_micropartition::MicroPartition::from_arrow(Arc::new(daft_schema), vec![sentinel])
            .map_err(|error| format!("PGDAFT_ZERO_FIELD_IMPORT: {error}"))?
            .eval_expression_list(&[])
            .map_err(|error| format!("PGDAFT_ZERO_FIELD_PROJECT: {error}"))?;
    if batch.len() != rows || !batch.schema().is_empty() {
        return Err("PGDAFT_ZERO_FIELD_CARDINALITY_MISMATCH".to_string());
    }
    Ok(Arc::new(batch))
}

unsafe fn release_accepted_arrow_batch(array: *mut ArrowArray, schema: *mut ArrowSchema) {
    if let Some(release) = unsafe { (*array).release } {
        unsafe { release(array) };
    }
    unsafe { (*array).release = None };
    if let Some(release) = unsafe { (*schema).release } {
        unsafe { release(schema) };
    }
    unsafe { (*schema).release = None };
}

fn checked_logical_bytes_add(left: u64, right: u64) -> Result<u64, String> {
    left.checked_add(right)
        .ok_or_else(|| "PGDAFT_ARROW_LOGICAL_BYTES_OVERFLOW".to_string())
}

fn checked_logical_bytes_mul(left: u64, right: u64) -> Result<u64, String> {
    left.checked_mul(right)
        .ok_or_else(|| "PGDAFT_ARROW_LOGICAL_BYTES_OVERFLOW".to_string())
}

unsafe fn validate_input_schema(
    schema: *mut ArrowSchema,
    expected: &ArrowSchemaType,
) -> Result<(), String> {
    if schema.is_null() {
        return Err("PGDAFT_ARROW_SCHEMA_NULL".to_string());
    }
    if unsafe { (*schema).release.is_none() } {
        return Err("PGDAFT_ARROW_SCHEMA_RELEASED".to_string());
    }
    let actual = ArrowSchemaType::try_from(unsafe { &*schema })
        .map_err(|error| format!("PGDAFT_ARROW_SCHEMA_INVALID: {error}"))?;
    if actual.fields().len() != expected.fields().len()
        || actual
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(actual, expected)| {
                actual.data_type() != expected.data_type()
                    || actual.is_nullable() != expected.is_nullable()
            })
    {
        return Err("PGDAFT_ARROW_SCHEMA_MISMATCH".to_string());
    }
    Ok(())
}

fn export_batch(
    batch: &daft_micropartition::MicroPartition,
    expected_schema: &ArrowSchemaType,
    schema_fingerprint: &str,
) -> Result<(ArrowArray, ArrowSchema), String> {
    let arrays = match batch
        .concat_or_get()
        .map_err(|error| format!("PGDAFT_ARROW_EXPORT_CONCAT: {error}"))?
    {
        Some(record_batch) => record_batch.get_inner_arrow_arrays().collect(),
        None => expected_schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect(),
    };
    // COUNT(*) above a Daft Filter legitimately consumes a zero-column batch
    // whose row count is still meaningful.  Preserve that length explicitly;
    // StructArray::new cannot infer it when there are no child arrays.
    let struct_array = StructArray::try_new_with_length(
        Fields::from(expected_schema.fields().to_vec()),
        arrays,
        None,
        batch.len(),
    )
    .map_err(|error| format!("PGDAFT_ARROW_EXPORT_STRUCT: {error}"))?;
    let (array, schema) =
        to_ffi(&struct_array.to_data()).map_err(|error| format!("PGDAFT_ARROW_EXPORT: {error}"))?;
    let schema = schema
        .with_metadata([(ARROW_SCHEMA_FINGERPRINT_KEY, schema_fingerprint)])
        .map_err(|error| format!("PGDAFT_ARROW_EXPORT_METADATA: {error}"))?;
    Ok((array, schema))
}

fn decode_digest(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).ok()?;
        out[index] = u8::from_str_radix(text, 16).ok()?;
    }
    Some(out)
}

/// JSON value deserializer that rejects duplicate object keys and floats.
struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueVisitor;

impl<'de> de::Visitor<'de> for UniqueValueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("canonical JSON without duplicate keys or floating-point values")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Err(E::custom("floating-point JSON numbers are forbidden"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(UniqueValue(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(UniqueValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut seen = HashSet::new();
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom(format!("duplicate object key '{key}'")));
            }
            let UniqueValue(value) = object.next_value()?;
            values.insert(key, value);
        }
        Ok(UniqueValue(Value::Object(values)))
    }
}
