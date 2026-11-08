//! End-to-end tests for the frozen v1 ABI and real Swordfish execution path.

use std::{
    ffi::{c_char, CStr, CString},
    fs,
    os::unix::fs::{symlink, DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
    ptr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use arrow::{
    array::{make_array, Array, ArrayRef, Int32Array, Int64Array, StructArray},
    datatypes::{DataType, Field, Fields, Schema as ArrowSchemaType},
    ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use daft_pg_bridge::{
    capabilities::CAPABILITY_DIGEST,
    error::DpgError,
    execution::{
        dpg_abi_major, dpg_abi_minor, dpg_cancel_v1, dpg_capabilities, dpg_capability_digest_v1,
        dpg_close_v1, dpg_error_free_v1, dpg_error_message_v1, dpg_finish_source_v1,
        dpg_metrics_snapshot_v1, dpg_open_v1, dpg_try_next_batch_v1, dpg_try_push_batch_v1,
        dpg_wait_v1, DpgConfigV1, DpgHandle, DpgMetricsV1, DpgStatus, DPG_EVENT_END,
        DPG_EVENT_OUTPUT_READY, DPG_SPILL_CLEANED, DPG_SPILL_NOT_ENABLED, DPG_SPILL_PENDING,
    },
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const SOURCE_PROJECT_LIMIT_FILTER_BITS: u64 = 0b0111_1111;
const SOURCE_PROJECT_BITS: u64 = 0b0001_1111;
const P1_HASH_JOIN_BITS: u64 = 0b1000_1111;
const P1_HASH_AGGREGATE_BITS: u64 = 0b1_0000_1111;
const P1_SORT_BITS: u64 = 0b10_0000_1111;
const P1_TOP_N_BITS: u64 = 0b100_0000_1111;
const ENABLED_RUNTIME_BITS: u64 = 0b111_1111_1111;
const SCHEMA_FINGERPRINT_KEY: &str = "pg_daft_schema_fingerprint_v1";

static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct SecureTestDirectory {
    path: PathBuf,
}

impl SecureTestDirectory {
    fn new(label: &str) -> Self {
        let id = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("pgdaft-{label}-{}-{id:016x}", std::process::id()));
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(&path).expect("create secure test directory");
        Self { path }
    }

    fn c_string(&self) -> CString {
        CString::new(self.path.to_str().expect("test path is UTF-8")).unwrap()
    }

    fn is_empty(&self) -> bool {
        fs::read_dir(&self.path)
            .expect("read secure test directory")
            .next()
            .is_none()
    }
}

impl Drop for SecureTestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn expected_output_schema_fingerprint() -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"PGDAFT_ARROW_SCHEMA_V1");
    hasher.update(1u32.to_be_bytes()); // transport contract version
    hasher.update(1u32.to_be_bytes()); // field count
    hasher.update(0u32.to_be_bytes()); // field index
    hasher.update(20u32.to_be_bytes()); // INT8OID
    hasher.update((-1i32 as u32).to_be_bytes());
    hasher.update(0u32.to_be_bytes()); // no collation
    hasher.update([0u8]); // NOT NULL
    hasher.update([1u8]); // Arrow format length
    hasher.update(b"l");
    format!("{:x}", hasher.finalize())
}

fn daft_compute_threads() -> Vec<String> {
    let mut names = fs::read_dir("/proc/self/task")
        .expect("Linux /proc task directory")
        .filter_map(Result::ok)
        .filter_map(|entry| fs::read_to_string(entry.path().join("comm")).ok())
        .map(|name| name.trim().to_owned())
        .filter(|name| name.starts_with("DAFTCPU-"))
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn int8_type() -> Value {
    json!({
        "pg_oid": 20,
        "typmod": -1,
        "collation_oid": 0,
        "nullable": false,
        "arrow_format": "l",
        "daft_dtype": "Int64"
    })
}

fn field(column_id: u32, name: &str) -> Value {
    json!({"column_id": column_id, "name": name, "type": int8_type()})
}

fn executable_plan() -> Vec<u8> {
    let source_field = field(1, "_pgd_f1_s0_c1");
    let output_field = field(2, "_pgd_f1_s0_c2");
    let mut value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": SOURCE_PROJECT_LIMIT_FILTER_BITS,
        "fragment_id": 1,
        "root_node_id": 4,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [source_field.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [source_field]},
            {
                "id": 2,
                "kind": "filter",
                "input": 1,
                "predicate": {
                    "kind": "compare",
                    "semantic_id": "gt",
                    "pg_oid": 413,
                    "args": [
                        {"kind": "input_ref", "column_id": 1, "type": int8_type()},
                        {
                            "kind": "literal",
                            "encoding": "i64",
                            "value": 10,
                            "type": int8_type()
                        }
                    ],
                    "type": {
                        "pg_oid": 16,
                        "typmod": -1,
                        "collation_oid": 0,
                        "nullable": false,
                        "arrow_format": "b",
                        "daft_dtype": "Boolean"
                    }
                }
            },
            {
                "id": 3,
                "kind": "project",
                "input": 2,
                "exprs": [{
                    "output": output_field.clone(),
                    "expr": {"kind": "input_ref", "column_id": 1, "type": int8_type()}
                }]
            },
            {"id": 4, "kind": "limit", "input": 3, "offset": 1, "count": 2}
        ],
        "output_schema": [output_field]
    });
    let canonical = serde_json::to_vec(&value).expect("canonical test JSON");
    let digest = format!("{:x}", Sha256::digest(canonical));
    value
        .as_object_mut()
        .unwrap()
        .insert("payload_sha256".to_string(), Value::String(digest));
    serde_json::to_vec(&value).expect("executable test plan")
}

fn identity_plan() -> Vec<u8> {
    let source_field = field(1, "_pgd_f1_s0_c1");
    let output_field = field(2, "_pgd_f1_s0_c2");
    let mut value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": SOURCE_PROJECT_BITS,
        "fragment_id": 1,
        "root_node_id": 2,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [source_field.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [source_field]},
            {
                "id": 2,
                "kind": "project",
                "input": 1,
                "exprs": [{
                    "output": output_field.clone(),
                    "expr": {"kind": "input_ref", "column_id": 1, "type": int8_type()}
                }]
            }
        ],
        "output_schema": [output_field]
    });
    let canonical = serde_json::to_vec(&value).expect("canonical identity JSON");
    let digest = format!("{:x}", Sha256::digest(canonical));
    value
        .as_object_mut()
        .unwrap()
        .insert("payload_sha256".to_string(), Value::String(digest));
    serde_json::to_vec(&value).expect("identity test plan")
}

fn sort_topn_plan(top_n: bool, limit: u64, offset: u64) -> Vec<u8> {
    let source_field = field(1, "_pgd_f1_s0_c1");
    let key = json!({
        "expr": {"kind": "input_ref", "column_id": 1, "type": int8_type()},
        "pg_sort_operator_oid": 412,
        "direction": "asc",
        "nulls_first": false,
        "collation_semantics": "none"
    });
    let root = if top_n {
        json!({
            "id": 2,
            "kind": "top_n",
            "input": 1,
            "keys": [key],
            "limit": limit,
            "offset": offset
        })
    } else {
        json!({"id": 2, "kind": "sort", "input": 1, "keys": [key]})
    };
    let mut value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": if top_n { P1_TOP_N_BITS } else { P1_SORT_BITS },
        "fragment_id": 1,
        "root_node_id": 2,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [source_field.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [source_field.clone()]},
            root
        ],
        "output_schema": [source_field]
    });
    let canonical = serde_json::to_vec(&value).expect("canonical Sort/TopN JSON");
    let digest = format!("{:x}", Sha256::digest(canonical));
    value
        .as_object_mut()
        .unwrap()
        .insert("payload_sha256".to_string(), Value::String(digest));
    serde_json::to_vec(&value).expect("Sort/TopN test plan")
}

fn left_join_empty_build_plan() -> Vec<u8> {
    let left = field(1, "_pgd_f1_s0_c1");
    let right = field(2, "_pgd_f1_s1_c2");
    let mut nullable_right = right.clone();
    nullable_right["type"]["nullable"] = json!(true);
    let mut value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": P1_HASH_JOIN_BITS,
        "fragment_id": 1,
        "root_node_id": 3,
        "pg_source_drive_order": [0, 1],
        "sources": [
            {"source_id": 0, "fields": [left.clone()]},
            {"source_id": 1, "fields": [right.clone()]}
        ],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [left.clone()]},
            {"id": 2, "kind": "source", "source_id": 1, "output": [right.clone()]},
            {
                "id": 3,
                "kind": "hash_join",
                "left": 1,
                "right": 2,
                "join_type": "left",
                "build_side": "right",
                "keys": [{
                    "left": {"kind": "input_ref", "column_id": 1, "type": int8_type()},
                    "right": {"kind": "input_ref", "column_id": 2, "type": int8_type()},
                    "null_equals_null": false
                }],
                "join_filter": null,
                "post_filter": null
            }
        ],
        "output_schema": [left, nullable_right]
    });
    let canonical = serde_json::to_vec(&value).expect("canonical empty-build join JSON");
    let digest = format!("{:x}", Sha256::digest(canonical));
    value
        .as_object_mut()
        .unwrap()
        .insert("payload_sha256".to_string(), Value::String(digest));
    serde_json::to_vec(&value).expect("empty-build join test plan")
}

fn int4_type(nullable: bool) -> Value {
    json!({
        "pg_oid": 23,
        "typmod": -1,
        "collation_oid": 0,
        "nullable": nullable,
        "arrow_format": "i",
        "daft_dtype": "Int32"
    })
}

fn hash_aggregate_plan(grouped: bool) -> Vec<u8> {
    let source = json!({
        "column_id": 1,
        "name": "_pgd_f1_s0_c1",
        "type": int4_type(true)
    });
    let group_keys = if grouped {
        json!([{
            "output": {
                "column_id": 10,
                "name": "_pgd_f1_s0_c10",
                "type": int4_type(true)
            },
            "expr": {
                "kind": "input_ref",
                "column_id": 1,
                "type": int4_type(true)
            }
        }])
    } else {
        json!([])
    };
    let aggregates = json!([
        {
            "semantic_id": "count_star",
            "pg_aggregate_oid": 2803,
            "args": [],
            "distinct": false,
            "order_by": [],
            "filter": null,
            "output": {
                "column_id": 11,
                "name": "_pgd_f1_s0_c11",
                "type": int8_type()
            }
        },
        {
            "semantic_id": "count_value",
            "pg_aggregate_oid": 2147,
            "args": [{
                "kind": "input_ref",
                "column_id": 1,
                "type": int4_type(true)
            }],
            "distinct": false,
            "order_by": [],
            "filter": null,
            "output": {
                "column_id": 12,
                "name": "_pgd_f1_s0_c12",
                "type": int8_type()
            }
        },
        {
            "semantic_id": "sum_int4",
            "pg_aggregate_oid": 2108,
            "args": [{
                "kind": "input_ref",
                "column_id": 1,
                "type": int4_type(true)
            }],
            "distinct": false,
            "order_by": [],
            "filter": null,
            "output": {
                "column_id": 13,
                "name": "_pgd_f1_s0_c13",
                "type": {
                    "pg_oid": 20,
                    "typmod": -1,
                    "collation_oid": 0,
                    "nullable": true,
                    "arrow_format": "l",
                    "daft_dtype": "Int64"
                }
            }
        },
        {
            "semantic_id": "min_int4",
            "pg_aggregate_oid": 2132,
            "args": [{
                "kind": "input_ref",
                "column_id": 1,
                "type": int4_type(true)
            }],
            "distinct": false,
            "order_by": [],
            "filter": null,
            "output": {
                "column_id": 14,
                "name": "_pgd_f1_s0_c14",
                "type": int4_type(true)
            }
        },
        {
            "semantic_id": "max_int4",
            "pg_aggregate_oid": 2116,
            "args": [{
                "kind": "input_ref",
                "column_id": 1,
                "type": int4_type(true)
            }],
            "distinct": false,
            "order_by": [],
            "filter": null,
            "output": {
                "column_id": 15,
                "name": "_pgd_f1_s0_c15",
                "type": int4_type(true)
            }
        }
    ]);
    let mut output_schema = Vec::new();
    if grouped {
        output_schema.push(json!({
            "column_id": 10,
            "name": "_pgd_f1_s0_c10",
            "type": int4_type(true)
        }));
    }
    output_schema.extend(
        aggregates
            .as_array()
            .unwrap()
            .iter()
            .map(|aggregate| aggregate.get("output").expect("aggregate output").clone()),
    );
    let mut value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": P1_HASH_AGGREGATE_BITS,
        "fragment_id": 1,
        "root_node_id": 2,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": [source.clone()]}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": [source]},
            {
                "id": 2,
                "kind": "hash_aggregate",
                "input": 1,
                "stage": "complete",
                "grouping_mode": "simple",
                "group_keys": group_keys,
                "aggregates": aggregates
            }
        ],
        "output_schema": output_schema
    });
    let canonical = serde_json::to_vec(&value).expect("canonical Aggregate JSON");
    let digest = format!("{:x}", Sha256::digest(canonical));
    value
        .as_object_mut()
        .unwrap()
        .insert("payload_sha256".to_string(), Value::String(digest));
    serde_json::to_vec(&value).expect("Aggregate test plan")
}

fn zero_field_count_star_plan() -> Vec<u8> {
    let count_output = json!({
        "column_id": 11,
        "name": "_pgd_f1_s0_c11",
        "type": int8_type()
    });
    let mut value = json!({
        "capability_registry_sha256": CAPABILITY_DIGEST,
        "required_runtime_capability_bits": P1_HASH_AGGREGATE_BITS,
        "fragment_id": 1,
        "root_node_id": 2,
        "pg_source_drive_order": [0],
        "sources": [{"source_id": 0, "fields": []}],
        "nodes": [
            {"id": 1, "kind": "source", "source_id": 0, "output": []},
            {
                "id": 2,
                "kind": "hash_aggregate",
                "input": 1,
                "stage": "complete",
                "grouping_mode": "simple",
                "group_keys": [],
                "aggregates": [{
                    "semantic_id": "count_star",
                    "pg_aggregate_oid": 2803,
                    "args": [],
                    "distinct": false,
                    "order_by": [],
                    "filter": null,
                    "output": count_output.clone()
                }]
            }
        ],
        "output_schema": [count_output]
    });
    let canonical = serde_json::to_vec(&value).expect("canonical zero-field Aggregate JSON");
    let digest = format!("{:x}", Sha256::digest(canonical));
    value
        .as_object_mut()
        .unwrap()
        .insert("payload_sha256".to_string(), Value::String(digest));
    serde_json::to_vec(&value).expect("zero-field Aggregate test plan")
}

fn aggregate_input_batch() -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(1),
        None,
        Some(2),
        None,
    ]));
    let field = Arc::new(Field::new("catalog_value", DataType::Int32, true));
    let batch = StructArray::from(vec![(field, values)]);
    to_ffi(&batch.to_data()).expect("export Aggregate input C Data")
}

fn zero_field_input_batch(rows: usize) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let fields = Fields::from(Vec::<Arc<Field>>::new());
    let batch = StructArray::try_new_with_length(fields, vec![], None, rows)
        .expect("construct zero-field input batch");
    to_ffi(&batch.to_data()).expect("export zero-field input C Data")
}

fn input_batch() -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    // PostgreSQL's TupleDesc exports the catalog name.  The Bridge validates
    // type/nullability, then rebinds this positional field to the stable IR
    // name `_pgd_f1_s0_c1` before Daft evaluates the Project expression.
    let field = Arc::new(Field::new("catalog_id", DataType::Int64, false));
    let batch = StructArray::from(vec![(field, values)]);
    to_ffi(&batch.to_data()).expect("export input C Data")
}

fn large_input_batch(rows: usize) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let field = Arc::new(Field::new("catalog_id", DataType::Int64, false));
    let batch = StructArray::from(vec![(field, values)]);
    to_ffi(&batch.to_data()).expect("export large input C Data")
}

fn int64_input_batch(values: Vec<i64>) -> (FFI_ArrowArray, FFI_ArrowSchema) {
    let values: ArrayRef = Arc::new(Int64Array::from(values));
    let field = Arc::new(Field::new("catalog_id", DataType::Int64, false));
    let batch = StructArray::from(vec![(field, values)]);
    to_ffi(&batch.to_data()).expect("export Sort/TopN input C Data")
}

unsafe fn free_error(error: &mut *mut DpgError) {
    unsafe { dpg_error_free_v1(error) };
    assert!(error.is_null());
}

unsafe fn assert_error_contains(error: &mut *mut DpgError, needle: &str) {
    assert!(!error.is_null());
    let message = unsafe { dpg_error_message_v1(*error) };
    assert!(!message.is_null());
    let message = unsafe { CStr::from_ptr(message) }
        .to_str()
        .expect("bridge error is UTF-8");
    assert!(
        message.contains(needle),
        "error '{message}' lacks '{needle}'"
    );
    unsafe { free_error(error) };
}

unsafe fn take_error_message(error: &mut *mut DpgError) -> String {
    if error.is_null() {
        return "<no bridge error>".to_string();
    }
    let message = unsafe { dpg_error_message_v1(*error) };
    let message = if message.is_null() {
        "<no bridge error message>".to_string()
    } else {
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    };
    unsafe { free_error(error) };
    message
}

fn metrics_snapshot_raw(handle: *mut DpgHandle, error: &mut *mut DpgError) -> DpgMetricsV1 {
    let mut metrics = DpgMetricsV1 {
        struct_size: std::mem::size_of::<DpgMetricsV1>() as u32,
        ..Default::default()
    };
    assert_eq!(
        unsafe { dpg_metrics_snapshot_v1(handle, &mut metrics, error) },
        DpgStatus::Ok
    );
    assert!(error.is_null());
    assert_eq!(metrics.reserved0, 0);
    assert_eq!(metrics.reserved1, 0);
    metrics
}

fn metrics_snapshot(handle: *mut DpgHandle, error: &mut *mut DpgError) -> DpgMetricsV1 {
    let metrics = metrics_snapshot_raw(handle, error);
    assert_eq!(metrics.spill_bytes, 0);
    assert_eq!(metrics.spill_count, 0);
    assert_eq!(metrics.spill_cleanup_status, DPG_SPILL_NOT_ENABLED);
    metrics
}

fn execute_sort_topn(
    top_n: bool,
    limit: u64,
    offset: u64,
    batches: Vec<Vec<i64>>,
    memory_limit_bytes: u64,
) -> Result<(Vec<i64>, DpgMetricsV1), String> {
    execute_sort_topn_configured(
        top_n,
        limit,
        offset,
        batches,
        memory_limit_bytes,
        ptr::null(),
        || {},
    )
}

fn execute_sort_topn_configured<F>(
    top_n: bool,
    limit: u64,
    offset: u64,
    batches: Vec<Vec<i64>>,
    memory_limit_bytes: u64,
    spill_dir: *const c_char,
    after_push: F,
) -> Result<(Vec<i64>, DpgMetricsV1), String>
where
    F: FnOnce(),
{
    let config = DpgConfigV1 {
        struct_size: std::mem::size_of::<DpgConfigV1>() as u32,
        worker_threads: 2,
        max_inflight_batches_per_source: 2,
        memory_limit_bytes,
        spill_dir,
        wait_timeout_ms: 20,
    };
    let plan = sort_topn_plan(top_n, limit, offset);
    let mut handle: *mut DpgHandle = ptr::null_mut();
    let mut error: *mut DpgError = ptr::null_mut();
    let status = unsafe {
        dpg_open_v1(
            plan.as_ptr(),
            plan.len() as u64,
            &config,
            &mut handle,
            &mut error,
        )
    };
    if status != DpgStatus::Ok {
        return Err(unsafe { take_error_message(&mut error) });
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    for values in batches {
        let (mut array, mut schema) = int64_input_batch(values);
        loop {
            match unsafe { dpg_try_push_batch_v1(handle, 0, &mut array, &mut schema, &mut error) } {
                DpgStatus::Ok => break,
                DpgStatus::Again => {
                    if Instant::now() >= deadline {
                        unsafe { dpg_close_v1(&mut handle) };
                        return Err("Sort/TopN input push timed out".to_string());
                    }
                    let mut events = 0;
                    let status = unsafe { dpg_wait_v1(handle, 20, &mut events, &mut error) };
                    if !matches!(status, DpgStatus::Ok | DpgStatus::Again) {
                        let message = unsafe { take_error_message(&mut error) };
                        unsafe { dpg_close_v1(&mut handle) };
                        return Err(message);
                    }
                }
                _ => {
                    let message = unsafe { take_error_message(&mut error) };
                    unsafe { dpg_close_v1(&mut handle) };
                    return Err(message);
                }
            }
        }
    }
    after_push();

    loop {
        match unsafe { dpg_finish_source_v1(handle, 0, &mut error) } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                if Instant::now() >= deadline {
                    unsafe { dpg_close_v1(&mut handle) };
                    return Err("Sort/TopN source finish timed out".to_string());
                }
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 20, &mut events, &mut error) };
                if !matches!(status, DpgStatus::Ok | DpgStatus::Again) {
                    let message = unsafe { take_error_message(&mut error) };
                    unsafe { dpg_close_v1(&mut handle) };
                    return Err(message);
                }
            }
            _ => {
                let message = unsafe { take_error_message(&mut error) };
                unsafe { dpg_close_v1(&mut handle) };
                return Err(message);
            }
        }
    }

    let mut values = Vec::new();
    loop {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        match unsafe { dpg_try_next_batch_v1(handle, &mut array, &mut schema, &mut error) } {
            DpgStatus::Ok => {
                let data = unsafe { from_ffi(array, &schema) }
                    .map_err(|error| format!("Sort/TopN Arrow import failed: {error}"))?;
                let output = make_array(data);
                let output = output
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Sort/TopN output is a struct array");
                let column = output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Sort/TopN output is int8");
                values.extend(column.values());
                drop(schema);
            }
            DpgStatus::Again => {
                if Instant::now() >= deadline {
                    unsafe { dpg_close_v1(&mut handle) };
                    return Err("Sort/TopN output timed out".to_string());
                }
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 20, &mut events, &mut error) };
                if status == DpgStatus::End {
                    break;
                }
                if !matches!(status, DpgStatus::Ok | DpgStatus::Again) {
                    let message = unsafe { take_error_message(&mut error) };
                    unsafe { dpg_close_v1(&mut handle) };
                    return Err(message);
                }
            }
            DpgStatus::End => break,
            _ => {
                let message = unsafe { take_error_message(&mut error) };
                unsafe { dpg_close_v1(&mut handle) };
                return Err(message);
            }
        }
    }
    let metrics = metrics_snapshot_raw(handle, &mut error);
    if spill_dir.is_null() {
        assert_eq!(metrics.spill_bytes, 0);
        assert_eq!(metrics.spill_count, 0);
        assert_eq!(metrics.spill_cleanup_status, DPG_SPILL_NOT_ENABLED);
    }
    unsafe { dpg_close_v1(&mut handle) };
    Ok((values, metrics))
}

fn open_spill_sort(spill_dir: &CStr, memory_limit_bytes: u64) -> (*mut DpgHandle, *mut DpgError) {
    let config = DpgConfigV1 {
        struct_size: std::mem::size_of::<DpgConfigV1>() as u32,
        worker_threads: 2,
        max_inflight_batches_per_source: 2,
        memory_limit_bytes,
        spill_dir: spill_dir.as_ptr(),
        wait_timeout_ms: 20,
    };
    let plan = sort_topn_plan(false, 0, 0);
    let mut handle = ptr::null_mut();
    let mut error = ptr::null_mut();
    assert_eq!(
        unsafe {
            dpg_open_v1(
                plan.as_ptr(),
                plan.len() as u64,
                &config,
                &mut handle,
                &mut error,
            )
        },
        DpgStatus::Ok
    );
    assert!(!handle.is_null());
    assert!(error.is_null());
    (handle, error)
}

fn push_spill_input(handle: *mut DpgHandle, error: &mut *mut DpgError) {
    let (mut array, mut schema) = int64_input_batch((0..512).rev().collect());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match unsafe { dpg_try_push_batch_v1(handle, 0, &mut array, &mut schema, error) } {
            DpgStatus::Ok => return,
            DpgStatus::Again => {
                assert!(Instant::now() < deadline, "spill input push timed out");
                thread::sleep(Duration::from_millis(2));
            }
            status => panic!("spill input push failed with {status:?}"),
        }
    }
}

fn wait_for_spill_file(base: &Path) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let query_dirs = fs::read_dir(base)
            .expect("read spill base")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        assert!(query_dirs.len() <= 1, "one handle owns one query directory");
        if let Some(query_dir) = query_dirs.first() {
            assert_eq!(
                fs::metadata(query_dir).unwrap().permissions().mode() & 0o777,
                0o700,
                "query spill directory must be owner-only"
            );
            if let Some(path) = fs::read_dir(query_dir)
                .expect("read query spill directory")
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| path.extension().is_some_and(|ext| ext == "arrow"))
            {
                assert_eq!(
                    fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                    0o600,
                    "spill runs must be owner-only"
                );
                return path;
            }
        }
        assert!(Instant::now() < deadline, "spill run was not created");
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn sort_topn_executes_global_prefix_empty_input_and_memory_gates() {
    let batches = vec![vec![9, 1, 5], vec![3, 8, 2]];
    let (sort_values, sort_metrics) =
        execute_sort_topn(false, 0, 0, batches.clone(), 64 * 1024 * 1024)
            .expect("Sort execution succeeds");
    assert_eq!(sort_values, vec![1, 2, 3, 5, 8, 9]);
    assert_eq!(sort_metrics.input_rows, 6);
    assert_eq!(sort_metrics.input_batches, 2);
    assert_eq!(sort_metrics.output_rows, 6);
    assert_eq!(sort_metrics.output_batches, 1);
    assert!(sort_metrics.current_memory_bytes <= sort_metrics.peak_memory_bytes);

    let (topn_values, topn_metrics) =
        execute_sort_topn(true, 3, 2, batches, 64 * 1024 * 1024).expect("TopN execution succeeds");
    assert_eq!(
        topn_values,
        vec![3, 5, 8],
        "TopN must retain the global limit+offset prefix across input batches"
    );
    assert_eq!(topn_metrics.input_rows, 6);
    assert_eq!(topn_metrics.input_batches, 2);
    assert_eq!(topn_metrics.output_rows, 3);
    assert_eq!(topn_metrics.output_batches, 1);

    let (empty_sort, empty_sort_metrics) = execute_sort_topn(false, 0, 0, vec![], 64 * 1024 * 1024)
        .expect("empty Sort execution succeeds");
    assert!(empty_sort.is_empty());
    assert_eq!(empty_sort_metrics.input_rows, 0);
    assert_eq!(empty_sort_metrics.output_rows, 0);
    let (empty_topn, empty_topn_metrics) = execute_sort_topn(true, 3, 2, vec![], 64 * 1024 * 1024)
        .expect("empty TopN execution succeeds");
    assert!(empty_topn.is_empty());
    assert_eq!(empty_topn_metrics.input_rows, 0);
    assert_eq!(empty_topn_metrics.output_rows, 0);

    let sort_error = execute_sort_topn(false, 0, 0, vec![(0..1024).rev().collect()], 256)
        .expect_err("Sort must reject retained state over half the fragment budget");
    assert!(
        sort_error.contains("PGDAFT_SORT_MEMORY_LIMIT"),
        "unexpected Sort memory error: {sort_error}"
    );

    assert_eq!(
        execute_sort_topn(
            true,
            3,
            2,
            vec![(0..512).rev().collect(), (512..1024).rev().collect()],
            2048,
        )
        .expect("bounded TopN stays within its retained-state budget")
        .0,
        vec![2, 3, 4]
    );
    let top_n_error = execute_sort_topn(true, 1024, 0, vec![(0..1024).rev().collect()], 256)
        .expect_err("TopN must reject an oversized retained prefix");
    assert!(
        top_n_error.contains("PGDAFT_TOP_N_MEMORY_LIMIT"),
        "unexpected TopN memory error: {top_n_error}"
    );
}

#[test]
fn spill_sort_is_correct_secure_accounted_and_cleaned_on_success() {
    let base = SecureTestDirectory::new("spill-success");
    let spill_dir = base.c_string();
    let (values, metrics) = execute_sort_topn_configured(
        false,
        0,
        0,
        vec![(0..512).rev().collect()],
        256,
        spill_dir.as_ptr(),
        || {
            let _ = wait_for_spill_file(&base.path);
        },
    )
    .expect("forced spill Sort succeeds");

    assert_eq!(values, (0..512).collect::<Vec<_>>());
    assert!(metrics.spill_bytes > 0);
    assert!(metrics.spill_count > 0);
    assert_eq!(metrics.spill_cleanup_status, DPG_SPILL_CLEANED);
    assert!(
        base.is_empty(),
        "success must remove the per-query directory"
    );
}

#[test]
fn spill_directory_is_cleaned_on_error_and_cancel() {
    let error_base = SecureTestDirectory::new("spill-error");
    let error_path = error_base.c_string();
    let (mut handle, mut error) = open_spill_sort(&error_path, 256);
    push_spill_input(handle, &mut error);
    let _ = wait_for_spill_file(&error_base.path);
    assert_eq!(
        unsafe { dpg_finish_source_v1(handle, 99, &mut error) },
        DpgStatus::Error
    );
    unsafe { assert_error_contains(&mut error, "PGDAFT_SOURCE_ID_INVALID") };
    let error_metrics = metrics_snapshot_raw(handle, &mut error);
    assert_eq!(error_metrics.spill_cleanup_status, DPG_SPILL_CLEANED);
    assert!(
        error_base.is_empty(),
        "error must remove the per-query directory"
    );
    unsafe { dpg_close_v1(&mut handle) };

    let cancel_base = SecureTestDirectory::new("spill-cancel");
    let cancel_path = cancel_base.c_string();
    let (mut handle, mut error) = open_spill_sort(&cancel_path, 256);
    push_spill_input(handle, &mut error);
    let _ = wait_for_spill_file(&cancel_base.path);
    let pending = metrics_snapshot_raw(handle, &mut error);
    assert_eq!(pending.spill_cleanup_status, DPG_SPILL_PENDING);
    unsafe { dpg_cancel_v1(handle) };
    assert!(cancel_base.is_empty(), "cancel must remove spill state");
    let cleaned = metrics_snapshot_raw(handle, &mut error);
    assert_eq!(cleaned.spill_cleanup_status, DPG_SPILL_CLEANED);
    unsafe { dpg_close_v1(&mut handle) };
}

#[test]
fn spill_base_rejects_insecure_mode_and_symlink_without_disclosure() {
    let insecure = SecureTestDirectory::new("spill-insecure");
    fs::set_permissions(&insecure.path, fs::Permissions::from_mode(0o777)).unwrap();
    let insecure_path = insecure.c_string();
    let config = DpgConfigV1 {
        struct_size: std::mem::size_of::<DpgConfigV1>() as u32,
        worker_threads: 2,
        max_inflight_batches_per_source: 2,
        memory_limit_bytes: 256,
        spill_dir: insecure_path.as_ptr(),
        wait_timeout_ms: 20,
    };
    let plan = sort_topn_plan(false, 0, 0);
    let mut handle = ptr::null_mut();
    let mut error = ptr::null_mut();
    assert_eq!(
        unsafe {
            dpg_open_v1(
                plan.as_ptr(),
                plan.len() as u64,
                &config,
                &mut handle,
                &mut error,
            )
        },
        DpgStatus::Error
    );
    let message = unsafe { take_error_message(&mut error) };
    assert_eq!(message, "PGDAFT_SPILL_BASE_INSECURE_MODE");
    assert!(!message.contains(insecure.path.to_str().unwrap()));
    assert!(handle.is_null());

    let target = SecureTestDirectory::new("spill-target");
    let link = target.path.with_extension("link");
    symlink(&target.path, &link).unwrap();
    let link_string = CString::new(link.to_str().unwrap()).unwrap();
    let config = DpgConfigV1 {
        spill_dir: link_string.as_ptr(),
        ..config
    };
    assert_eq!(
        unsafe {
            dpg_open_v1(
                plan.as_ptr(),
                plan.len() as u64,
                &config,
                &mut handle,
                &mut error,
            )
        },
        DpgStatus::Error
    );
    let message = unsafe { take_error_message(&mut error) };
    assert_eq!(message, "PGDAFT_SPILL_BASE_SYMLINK");
    assert!(!message.contains(link.to_str().unwrap()));
    assert!(handle.is_null());
    fs::remove_file(link).unwrap();
}

fn execute_aggregate_plan(grouped: bool, push_input: bool) -> Vec<ArrayRef> {
    let config = DpgConfigV1 {
        struct_size: std::mem::size_of::<DpgConfigV1>() as u32,
        worker_threads: 2,
        max_inflight_batches_per_source: 2,
        memory_limit_bytes: 64 * 1024 * 1024,
        spill_dir: ptr::null(),
        wait_timeout_ms: 100,
    };
    let plan = hash_aggregate_plan(grouped);
    let mut handle: *mut DpgHandle = ptr::null_mut();
    let mut error: *mut DpgError = ptr::null_mut();
    let status = unsafe {
        dpg_open_v1(
            plan.as_ptr(),
            plan.len() as u64,
            &config,
            &mut handle,
            &mut error,
        )
    };
    if status != DpgStatus::Ok {
        unsafe { assert_error_contains(&mut error, "PGDAFT") };
        panic!("Aggregate open failed with {status:?}");
    }

    if push_input {
        let (mut array, mut schema) = aggregate_input_batch();
        loop {
            match unsafe { dpg_try_push_batch_v1(handle, 0, &mut array, &mut schema, &mut error) } {
                DpgStatus::Ok => break,
                DpgStatus::Again => {
                    let mut events = 0;
                    let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                    assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
                }
                status => panic!("Aggregate input push failed with {status:?}"),
            }
        }
    }

    loop {
        match unsafe { dpg_finish_source_v1(handle, 0, &mut error) } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
            }
            status => panic!("Aggregate source finish failed with {status:?}"),
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut outputs = Vec::new();
    loop {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        match unsafe { dpg_try_next_batch_v1(handle, &mut array, &mut schema, &mut error) } {
            DpgStatus::Ok => {
                let exported_schema = ArrowSchemaType::try_from(&schema)
                    .expect("Aggregate output C Data schema is valid");
                let expected_nullability = if grouped {
                    vec![true, false, false, true, true, true]
                } else {
                    vec![false, false, true, true, true]
                };
                assert_eq!(
                    exported_schema
                        .fields()
                        .iter()
                        .map(|field| field.is_nullable())
                        .collect::<Vec<_>>(),
                    expected_nullability,
                    "Aggregate Arrow output must preserve frozen IR nullability"
                );
                let data = unsafe { from_ffi(array, &schema) }.expect("import Aggregate output");
                outputs.push(make_array(data));
                drop(schema);
            }
            DpgStatus::Again => {
                assert!(Instant::now() < deadline, "HashAggregate execution hung");
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                if !matches!(status, DpgStatus::Ok | DpgStatus::Again | DpgStatus::End) {
                    let message = unsafe { dpg_error_message_v1(error) };
                    let message = if message.is_null() {
                        "<no error>".into()
                    } else {
                        unsafe { CStr::from_ptr(message) }
                            .to_string_lossy()
                            .into_owned()
                    };
                    panic!("HashAggregate wait failed with {status:?}: {message}");
                }
                if status == DpgStatus::End {
                    break;
                }
            }
            DpgStatus::End => break,
            status => {
                unsafe { assert_error_contains(&mut error, "PGDAFT") };
                panic!("HashAggregate output failed with {status:?}");
            }
        }
    }
    let metrics = metrics_snapshot(handle, &mut error);
    assert_eq!(metrics.input_rows, if push_input { 5 } else { 0 });
    assert_eq!(metrics.input_batches, if push_input { 1 } else { 0 });
    assert_eq!(
        metrics.output_rows,
        outputs.iter().map(|array| array.len() as u64).sum::<u64>()
    );
    assert!(metrics.current_memory_bytes <= metrics.peak_memory_bytes);
    unsafe { dpg_close_v1(&mut handle) };
    assert!(handle.is_null());
    assert!(error.is_null());
    outputs
}

#[test]
fn hash_aggregate_count_star_accepts_zero_field_nonzero_row_batch() {
    let config = DpgConfigV1 {
        struct_size: std::mem::size_of::<DpgConfigV1>() as u32,
        worker_threads: 2,
        max_inflight_batches_per_source: 2,
        memory_limit_bytes: 64 * 1024 * 1024,
        spill_dir: ptr::null(),
        wait_timeout_ms: 100,
    };
    let plan = zero_field_count_star_plan();
    let mut handle: *mut DpgHandle = ptr::null_mut();
    let mut error: *mut DpgError = ptr::null_mut();
    assert_eq!(
        unsafe {
            dpg_open_v1(
                plan.as_ptr(),
                plan.len() as u64,
                &config,
                &mut handle,
                &mut error,
            )
        },
        DpgStatus::Ok
    );

    let (mut array, mut schema) = zero_field_input_batch(7);
    let status = unsafe { dpg_try_push_batch_v1(handle, 0, &mut array, &mut schema, &mut error) };
    if status != DpgStatus::Ok {
        let message = unsafe { take_error_message(&mut error) };
        panic!("zero-field Count(*) input failed with {status:?}: {message}");
    }
    assert!(array.release.is_none());
    assert!(schema.release.is_none());
    assert_eq!(
        unsafe { dpg_finish_source_v1(handle, 0, &mut error) },
        DpgStatus::Ok
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let output = loop {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        match unsafe { dpg_try_next_batch_v1(handle, &mut array, &mut schema, &mut error) } {
            DpgStatus::Ok => {
                let data =
                    unsafe { from_ffi(array, &schema) }.expect("import zero-field Count(*) output");
                break make_array(data);
            }
            DpgStatus::Again => {
                assert!(Instant::now() < deadline, "zero-field Count(*) hung");
                let mut events = 0;
                assert!(matches!(
                    unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) },
                    DpgStatus::Ok | DpgStatus::Again | DpgStatus::End
                ));
            }
            status => panic!("zero-field Count(*) output failed with {status:?}"),
        }
    };
    let output = output
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Count(*) output is a struct array");
    assert_eq!(output.len(), 1);
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Count(*) output is int8")
            .value(0),
        7
    );

    let metrics = metrics_snapshot(handle, &mut error);
    assert_eq!(metrics.input_rows, 7);
    assert_eq!(metrics.input_batches, 1);
    assert_eq!(metrics.input_bytes, 0);
    assert_eq!(metrics.output_rows, 1);
    unsafe { dpg_close_v1(&mut handle) };
    assert!(handle.is_null());
    assert!(error.is_null());
}

#[test]
fn hash_aggregate_executes_group_null_duplicates_and_empty_global_input() {
    let mut grouped_rows = Vec::new();
    for output in execute_aggregate_plan(true, true) {
        let output = output
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Aggregate output is a struct array");
        let group = output
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let count_star = output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let count_value = output
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sum = output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let min = output
            .column(4)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let max = output
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..output.len() {
            grouped_rows.push((
                (!group.is_null(row)).then(|| group.value(row)),
                count_star.value(row),
                count_value.value(row),
                (!sum.is_null(row)).then(|| sum.value(row)),
                (!min.is_null(row)).then(|| min.value(row)),
                (!max.is_null(row)).then(|| max.value(row)),
            ));
        }
    }
    grouped_rows.sort();
    assert_eq!(
        grouped_rows,
        vec![
            (None, 2, 0, None, None, None),
            (Some(1), 2, 2, Some(2), Some(1), Some(1)),
            (Some(2), 1, 1, Some(2), Some(2), Some(2)),
        ]
    );

    let global = execute_aggregate_plan(false, false);
    let global_rows = global.iter().map(|batch| batch.len()).sum::<usize>();
    assert_eq!(
        global_rows, 1,
        "global Aggregate over empty input emits one row"
    );
    let output = global
        .iter()
        .find(|batch| !batch.is_empty())
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert!(output.column(2).is_null(0));
    assert!(output.column(3).is_null(0));
    assert!(output.column(4).is_null(0));

    let grouped_empty = execute_aggregate_plan(true, false);
    assert!(
        grouped_empty.is_empty(),
        "GROUP BY over an empty PG source must return zero rows"
    );
}

#[test]
fn capability_thread_gate_and_real_execution_lifecycle() {
    assert!(
        daft_compute_threads().is_empty(),
        "test process unexpectedly started Daft runtime before ABI query"
    );

    assert_eq!(dpg_abi_major(), 1);
    assert_eq!(dpg_abi_minor(), 1);
    assert_eq!(dpg_capabilities(), ENABLED_RUNTIME_BITS);
    let mut digest = [0u8; 32];
    assert_eq!(
        unsafe { dpg_capability_digest_v1(digest.as_mut_ptr()) },
        DpgStatus::Ok
    );
    assert_eq!(hex::encode(digest), CAPABILITY_DIGEST);
    assert!(
        daft_compute_threads().is_empty(),
        "capability query must not initialize the Daft runtime"
    );

    let config = DpgConfigV1 {
        struct_size: std::mem::size_of::<DpgConfigV1>() as u32,
        worker_threads: 2,
        max_inflight_batches_per_source: 2,
        memory_limit_bytes: 64 * 1024 * 1024,
        spill_dir: ptr::null(),
        wait_timeout_ms: 100,
    };
    let plan = executable_plan();

    // Validation failures happen before runtime initialization and preserve a
    // NULL output handle.
    let mut bad_plan = plan.clone();
    let index = bad_plan
        .windows(64)
        .position(|window| window.iter().all(u8::is_ascii_hexdigit))
        .expect("plan contains a digest");
    bad_plan[index] = if bad_plan[index] == b'0' { b'1' } else { b'0' };
    let mut handle: *mut DpgHandle = ptr::null_mut();
    let mut error: *mut DpgError = ptr::null_mut();
    assert_eq!(
        unsafe {
            dpg_open_v1(
                bad_plan.as_ptr(),
                bad_plan.len() as u64,
                &config,
                &mut handle,
                &mut error,
            )
        },
        DpgStatus::Error
    );
    assert!(handle.is_null());
    unsafe { assert_error_contains(&mut error, "PAYLOAD_SHA256_MISMATCH") };
    assert!(daft_compute_threads().is_empty());

    let open_status = unsafe {
        dpg_open_v1(
            plan.as_ptr(),
            plan.len() as u64,
            &config,
            &mut handle,
            &mut error,
        )
    };
    if open_status != DpgStatus::Ok {
        let message = unsafe { dpg_error_message_v1(error) };
        let message = if message.is_null() {
            "<no error>".to_string()
        } else {
            unsafe { CStr::from_ptr(message) }
                .to_string_lossy()
                .into_owned()
        };
        panic!("open failed with {open_status:?}: {message}");
    }
    assert!(!handle.is_null());
    assert!(error.is_null());

    let deadline = Instant::now() + Duration::from_secs(2);
    while daft_compute_threads().len() < config.worker_threads as usize && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    let runtime_threads = daft_compute_threads().len();
    assert!(
        (config.worker_threads as usize..=config.worker_threads as usize + 1)
            .contains(&runtime_threads),
        "successful open must create the configured workers and at most Tokio's one bounded blocking helper; got {runtime_threads}"
    );

    let (mut input_array, mut input_schema) = input_batch();
    assert_eq!(
        unsafe {
            dpg_try_push_batch_v1(handle, 0, &mut input_array, &mut input_schema, &mut error)
        },
        DpgStatus::Ok
    );
    assert!(input_array.release.is_none());
    assert!(input_schema.release.is_none());

    loop {
        match unsafe { dpg_finish_source_v1(handle, 0, &mut error) } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 10, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
            }
            status => panic!("finish failed with {status:?}"),
        }
    }

    let mut output_array = FFI_ArrowArray::empty();
    let mut output_schema = FFI_ArrowSchema::empty();
    loop {
        match unsafe {
            dpg_try_next_batch_v1(handle, &mut output_array, &mut output_schema, &mut error)
        } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
                if status == DpgStatus::Ok {
                    assert_ne!(events & DPG_EVENT_OUTPUT_READY, 0);
                }
            }
            status => panic!("next failed with {status:?}"),
        }
    }
    assert!(output_array.release.is_some());
    assert!(output_schema.release.is_some());
    let exported_schema =
        ArrowSchemaType::try_from(&output_schema).expect("output C Data schema is valid");
    let expected_fingerprint = expected_output_schema_fingerprint();
    assert_eq!(
        exported_schema.metadata().get(SCHEMA_FINGERPRINT_KEY),
        Some(&expected_fingerprint),
        "output must carry the frozen PG/Arrow schema fingerprint"
    );
    let output_data = unsafe { from_ffi(output_array, &output_schema) }.expect("import output");
    let output = make_array(output_data);
    let output = output
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("top-level output is a RecordBatch struct");
    assert_eq!(output.column_names(), vec!["_pgd_f1_s0_c2"]);
    let values = output
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.values().as_ref(), &[30, 40]);
    drop(output_schema);

    loop {
        let mut empty_array = FFI_ArrowArray::empty();
        let mut empty_schema = FFI_ArrowSchema::empty();
        match unsafe {
            dpg_try_next_batch_v1(handle, &mut empty_array, &mut empty_schema, &mut error)
        } {
            DpgStatus::End => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Again | DpgStatus::End));
                if status == DpgStatus::End {
                    assert_ne!(events & DPG_EVENT_END, 0);
                    break;
                }
            }
            status => panic!("terminal poll failed with {status:?}"),
        }
    }
    let mut empty_array = FFI_ArrowArray::empty();
    let mut empty_schema = FFI_ArrowSchema::empty();
    assert_eq!(
        unsafe { dpg_try_next_batch_v1(handle, &mut empty_array, &mut empty_schema, &mut error,) },
        DpgStatus::End,
        "END must be permanent"
    );

    let mut short_metrics = DpgMetricsV1::default();
    assert_eq!(
        unsafe { dpg_metrics_snapshot_v1(handle, &mut short_metrics, &mut error) },
        DpgStatus::Incompatible
    );
    unsafe { assert_error_contains(&mut error, "PGDAFT_METRICS_SIZE_INCOMPATIBLE") };

    let metrics = metrics_snapshot(handle, &mut error);
    assert_eq!(metrics.input_rows, 4);
    assert_eq!(metrics.input_batches, 1);
    assert_eq!(metrics.input_bytes, 32);
    assert_eq!(metrics.output_rows, 2);
    assert_eq!(metrics.output_batches, 1);
    assert_eq!(metrics.output_bytes, 16);
    assert!(metrics.current_memory_bytes <= metrics.peak_memory_bytes);
    assert_eq!(metrics_snapshot(handle, &mut error), metrics);

    unsafe {
        dpg_cancel_v1(handle);
        dpg_cancel_v1(handle);
        dpg_close_v1(&mut handle);
        dpg_close_v1(&mut handle);
    }
    assert!(handle.is_null());
    assert!(error.is_null());

    // A duplicate-heavy HashJoin can produce one Daft MicroPartition larger
    // than PostgreSQL's frozen 65536-row Arrow batch ceiling. Exercise the
    // same Bridge boundary with an identity plan: the ABI must return two
    // bounded batches, retain the tail in the handle, and make wait report it
    // as immediately readable without polling the Swordfish output queue.
    let plan = identity_plan();
    let open_status = unsafe {
        dpg_open_v1(
            plan.as_ptr(),
            plan.len() as u64,
            &config,
            &mut handle,
            &mut error,
        )
    };
    if open_status != DpgStatus::Ok {
        let message = unsafe { dpg_error_message_v1(error) };
        let message = if message.is_null() {
            "<no error>".to_string()
        } else {
            unsafe { CStr::from_ptr(message) }
                .to_string_lossy()
                .into_owned()
        };
        panic!("large-output open failed with {open_status:?}: {message}");
    }
    let (mut input_array, mut input_schema) = large_input_batch(65537);
    loop {
        match unsafe {
            dpg_try_push_batch_v1(handle, 0, &mut input_array, &mut input_schema, &mut error)
        } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
            }
            status => panic!("large input push failed with {status:?}"),
        }
    }
    loop {
        match unsafe { dpg_finish_source_v1(handle, 0, &mut error) } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
            }
            status => panic!("large input finish failed with {status:?}"),
        }
    }

    let mut observed = Vec::new();
    for expected_rows in [65536usize, 1] {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        loop {
            match unsafe { dpg_try_next_batch_v1(handle, &mut array, &mut schema, &mut error) } {
                DpgStatus::Ok => break,
                DpgStatus::Again => {
                    let mut events = 0;
                    let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                    assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
                }
                status => panic!("large output next failed with {status:?}"),
            }
        }
        assert_eq!(array.length, expected_rows as i64);
        let data = unsafe { from_ffi(array, &schema) }.expect("import bounded output");
        let output = make_array(data);
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        observed.extend(values.values());
        drop(schema);

        if expected_rows == 65536 {
            let mut events = 0;
            assert_eq!(
                unsafe { dpg_wait_v1(handle, 0, &mut events, &mut error) },
                DpgStatus::Ok
            );
            assert_ne!(events & DPG_EVENT_OUTPUT_READY, 0);
        }
    }
    assert_eq!(observed.len(), 65537);
    assert_eq!(observed.first(), Some(&0));
    assert_eq!(observed.last(), Some(&65536));

    unsafe { dpg_close_v1(&mut handle) };
    assert!(handle.is_null());
    assert!(error.is_null());

    // An empty right/build source sends Finish without a preceding batch.
    // HashJoin must still finalize an empty hash table so that the left/probe
    // source can emit every row with a NULL-extended right column.
    let plan = left_join_empty_build_plan();
    assert_eq!(
        unsafe {
            dpg_open_v1(
                plan.as_ptr(),
                plan.len() as u64,
                &config,
                &mut handle,
                &mut error,
            )
        },
        DpgStatus::Ok
    );

    loop {
        match unsafe { dpg_finish_source_v1(handle, 1, &mut error) } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
            }
            status => panic!("empty build finish failed with {status:?}"),
        }
    }
    let (mut left_array, mut left_schema) = input_batch();
    loop {
        match unsafe {
            dpg_try_push_batch_v1(handle, 0, &mut left_array, &mut left_schema, &mut error)
        } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
            }
            status => panic!("left probe push failed with {status:?}"),
        }
    }
    loop {
        match unsafe { dpg_finish_source_v1(handle, 0, &mut error) } {
            DpgStatus::Ok => break,
            DpgStatus::Again => {
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                assert!(matches!(status, DpgStatus::Ok | DpgStatus::Again));
            }
            status => panic!("left probe finish failed with {status:?}"),
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut joined_rows = 0;
    loop {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();
        match unsafe { dpg_try_next_batch_v1(handle, &mut array, &mut schema, &mut error) } {
            DpgStatus::Ok => {
                let data = unsafe { from_ffi(array, &schema) }.expect("import empty-build output");
                let output = make_array(data);
                let output = output.as_any().downcast_ref::<StructArray>().unwrap();
                let left_values = output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let right_values = output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(left_values.values().as_ref(), &[10, 20, 30, 40]);
                assert_eq!(right_values.null_count(), 4);
                joined_rows += output.len();
                drop(schema);
            }
            DpgStatus::Again => {
                assert!(Instant::now() < deadline, "empty-build Left HashJoin hung");
                let mut events = 0;
                let status = unsafe { dpg_wait_v1(handle, 100, &mut events, &mut error) };
                if !matches!(status, DpgStatus::Ok | DpgStatus::Again | DpgStatus::End) {
                    let message = unsafe { dpg_error_message_v1(error) };
                    let message = if message.is_null() {
                        "<no error>".into()
                    } else {
                        unsafe { CStr::from_ptr(message) }
                            .to_string_lossy()
                            .into_owned()
                    };
                    panic!("empty-build wait failed with {status:?}: {message}");
                }
                if status == DpgStatus::End {
                    break;
                }
            }
            DpgStatus::End => break,
            status => panic!("empty-build output failed with {status:?}"),
        }
    }
    assert_eq!(joined_rows, 4);

    let join_metrics = metrics_snapshot(handle, &mut error);
    assert_eq!(join_metrics.input_rows, 4);
    assert_eq!(join_metrics.input_batches, 1);
    assert_eq!(join_metrics.output_rows, 4);
    assert_eq!(join_metrics.output_batches, 1);

    unsafe { dpg_close_v1(&mut handle) };
    assert!(handle.is_null());
    assert!(error.is_null());
}
