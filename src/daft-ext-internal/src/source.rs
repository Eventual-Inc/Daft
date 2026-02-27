//! Host-side adapter for extension scan sources.
//!
//! Wraps `FFI_ScanSource` vtables (from loaded extension modules) into
//! safe Rust types that Daft's scan pipeline can use.

use std::{
    collections::HashMap,
    ffi::{CStr, c_char, c_void},
    sync::{Arc, Mutex, OnceLock},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use common_error::{DaftError, DaftResult};
use daft_ext_abi::{
    FFI_ArrowArray, FFI_ArrowSchema, FFI_Pushdowns, FFI_SCAN_DONE, FFI_SCAN_ERROR, FFI_SCAN_OK,
    FFI_ScanSource,
};

use crate::module::ModuleHandle;

/// Bundles an `FFI_ScanSource` vtable with its parent `ModuleHandle`.
struct Inner {
    ffi: FFI_ScanSource,
    module: Arc<ModuleHandle>,
}

impl Inner {
    /// Read and free an FFI-allocated C string, returning the owned Rust string.
    unsafe fn take_ffi_string(&self, ptr: *mut c_char) -> String {
        let s = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { self.module.free_string(ptr) };
        s
    }

    /// Check an FFI return code, converting a non-zero result into a `DaftError`.
    fn check(&self, rc: i32, errmsg: *mut c_char, default_msg: &str) -> DaftResult<()> {
        if rc == 0 {
            return Ok(());
        }
        let msg = if errmsg.is_null() {
            default_msg.to_string()
        } else {
            unsafe { self.take_ffi_string(errmsg) }
        };
        Err(DaftError::InternalError(msg))
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        unsafe { (self.ffi.fini)(self.ffi.ctx.cast_mut()) };
    }
}

// SAFETY: FFI_ScanSource is already Send + Sync; ModuleHandle is Send + Sync.
unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

/// Host-side handle to an extension scan source.
///
/// Wraps the FFI vtable and provides safe methods for schema inference,
/// task creation, and batch reading.
pub struct ScanSourceHandle {
    name: String,
    inner: Arc<Inner>,
}

impl ScanSourceHandle {
    /// Returns the source name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the output schema for the given options (JSON string).
    pub fn schema(&self, options: &str) -> DaftResult<SchemaRef> {
        let mut ret_schema = FFI_ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (self.inner.ffi.schema)(
                self.inner.ffi.ctx,
                options.as_ptr().cast(),
                options.len(),
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        self.inner
            .check(rc, errmsg, "unknown error in extension schema")?;

        let arrow_schema = arrow_schema::Schema::try_from(&ret_schema)
            .map_err(|e| DaftError::InternalError(format!("schema import failed: {e}")))?;

        Ok(Arc::new(arrow_schema))
    }

    /// Get the number of tasks (partitions) for the given options.
    pub fn num_tasks(&self, options: &str) -> DaftResult<u32> {
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let n = unsafe {
            (self.inner.ffi.num_tasks)(
                self.inner.ffi.ctx,
                options.as_ptr().cast(),
                options.len(),
                &raw mut errmsg,
            )
        };
        if !errmsg.is_null() {
            let msg = unsafe { self.inner.take_ffi_string(errmsg) };
            return Err(DaftError::InternalError(msg));
        }
        Ok(n)
    }

    /// Create a task handle for the given partition index.
    pub fn open_task(
        &self,
        options: &str,
        task_index: u32,
        columns: Option<&[String]>,
        limit: Option<usize>,
    ) -> DaftResult<SourceTaskHandle> {
        // Build FFI_Pushdowns.
        let c_columns: Vec<std::ffi::CString>;
        let c_column_ptrs: Vec<*const c_char>;

        let pushdowns = if columns.is_some() || limit.is_some() {
            c_columns = columns
                .unwrap_or(&[])
                .iter()
                .map(|s| std::ffi::CString::new(s.as_str()).unwrap())
                .collect();
            c_column_ptrs = c_columns.iter().map(|c| c.as_ptr()).collect();

            FFI_Pushdowns {
                columns: if c_column_ptrs.is_empty() {
                    std::ptr::null()
                } else {
                    c_column_ptrs.as_ptr()
                },
                columns_count: c_column_ptrs.len(),
                limit: limit.map(|l| l as u64).unwrap_or(u64::MAX),
            }
        } else {
            c_columns = Vec::new();
            c_column_ptrs = Vec::new();
            let _ = (&c_columns, &c_column_ptrs); // suppress unused warnings
            FFI_Pushdowns {
                columns: std::ptr::null(),
                columns_count: 0,
                limit: u64::MAX,
            }
        };

        let mut ret_task: *mut c_void = std::ptr::null_mut();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (self.inner.ffi.create_task)(
                self.inner.ffi.ctx,
                options.as_ptr().cast(),
                options.len(),
                task_index,
                &raw const pushdowns,
                &raw mut ret_task,
                &raw mut errmsg,
            )
        };
        self.inner
            .check(rc, errmsg, "unknown error in extension create_task")?;

        Ok(SourceTaskHandle {
            task: ret_task,
            inner: self.inner.clone(),
        })
    }
}

/// Handle to an open extension scan task (reader).
///
/// Wraps the opaque FFI task pointer and provides `next_batch()`.
pub struct SourceTaskHandle {
    task: *mut c_void,
    inner: Arc<Inner>,
}

impl SourceTaskHandle {
    /// Read the next batch from the task.
    ///
    /// Returns `Ok(Some(batch))` for data, `Ok(None)` when exhausted.
    pub fn next_batch(&mut self) -> DaftResult<Option<RecordBatch>> {
        let mut ret_array = FFI_ArrowArray::empty();
        let mut ret_schema = FFI_ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (self.inner.ffi.next)(
                self.task,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };

        match rc {
            FFI_SCAN_OK => {
                // Import the Arrow struct array and convert to RecordBatch.
                let data = unsafe { arrow::ffi::from_ffi(ret_array, &ret_schema) }
                    .map_err(|e| DaftError::InternalError(format!("Arrow FFI import: {e}")))?;
                let array = arrow_array::make_array(data);
                let struct_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .ok_or_else(|| {
                        DaftError::InternalError(
                            "extension source returned non-struct array".to_string(),
                        )
                    })?;
                let batch = RecordBatch::from(struct_array);
                Ok(Some(batch))
            }
            FFI_SCAN_DONE => Ok(None),
            FFI_SCAN_ERROR => {
                let msg = if errmsg.is_null() {
                    "unknown error in extension next".to_string()
                } else {
                    unsafe { self.inner.take_ffi_string(errmsg) }
                };
                Err(DaftError::InternalError(msg))
            }
            other => Err(DaftError::InternalError(format!(
                "unexpected scan status code: {other}"
            ))),
        }
    }
}

impl Drop for SourceTaskHandle {
    fn drop(&mut self) {
        unsafe { (self.inner.ffi.close_task)(self.task) };
    }
}

// SAFETY: The task pointer is only accessed through &mut self methods.
unsafe impl Send for SourceTaskHandle {}

// ---------------------------------------------------------------------------
// Global source registry
// ---------------------------------------------------------------------------

static SOURCES: OnceLock<Mutex<HashMap<String, Arc<ScanSourceHandle>>>> = OnceLock::new();

fn sources() -> &'static Mutex<HashMap<String, Arc<ScanSourceHandle>>> {
    SOURCES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a scan source in the global registry.
pub fn register_source(handle: Arc<ScanSourceHandle>) {
    let name = handle.name().to_string();
    sources().lock().unwrap().insert(name, handle);
}

/// Look up a scan source by name.
pub fn get_source(name: &str) -> DaftResult<Arc<ScanSourceHandle>> {
    sources().lock().unwrap().get(name).cloned().ok_or_else(|| {
        DaftError::InternalError(format!("extension source '{name}' not found in registry"))
    })
}

/// Create a `ScanSourceHandle` from an FFI vtable and module handle,
/// and return it wrapped in an Arc (ready for registration).
pub fn into_scan_source_handle(
    ffi: FFI_ScanSource,
    module: Arc<ModuleHandle>,
) -> Arc<ScanSourceHandle> {
    let name_ptr = unsafe { (ffi.name)(ffi.ctx) };
    let name = unsafe { CStr::from_ptr(name_ptr) }
        .to_str()
        .expect("FFI source name must be valid UTF-8")
        .to_string();
    Arc::new(ScanSourceHandle {
        name,
        inner: Arc::new(Inner { ffi, module }),
    })
}
