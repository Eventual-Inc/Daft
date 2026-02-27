use std::{
    ffi::{CStr, c_char, c_int, c_void},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_schema::Field;
use daft_ext_abi::{
    FFI_ArrowArray, FFI_ArrowSchema, FFI_Pushdowns, FFI_ScanSource, FFI_SCAN_DONE, FFI_SCAN_ERROR,
    FFI_SCAN_OK,
};

use crate::{
    error::{DaftError, DaftResult},
    ffi::trampoline::trampoline,
};

/// Simplified pushdowns passed to scan sources.
pub struct ScanPushdowns {
    /// Projected column names, or `None` for all columns.
    pub columns: Option<Vec<String>>,
    /// Row limit, or `None` for no limit.
    pub limit: Option<usize>,
}

/// Trait for a single scan task that produces record batches.
pub trait DaftSourceTask: Send {
    /// Returns the next record batch, or `None` when exhausted.
    fn next_batch(&mut self) -> DaftResult<Option<RecordBatch>>;
}

/// Trait that extension authors implement to define a scan source.
pub trait DaftSource: Send + Sync {
    /// Source name (null-terminated, e.g. `c"my_source"`).
    fn name(&self) -> &CStr;

    /// Output schema for the given options.
    fn schema(&self, options: &str) -> DaftResult<Vec<Field>>;

    /// Number of tasks (partitions) for the given options.
    fn num_tasks(&self, options: &str) -> u32;

    /// Create a task (reader) for a partition.
    fn create_task(
        &self,
        options: &str,
        task_index: u32,
        pushdowns: &ScanPushdowns,
    ) -> DaftResult<Box<dyn DaftSourceTask>>;
}

/// A shared, type-erased scan source reference.
pub type DaftSourceRef = Arc<dyn DaftSource>;

/// Convert a [`DaftSourceRef`] into a [`FFI_ScanSource`] vtable.
///
/// The `Arc` is moved into the vtable's opaque context and released
/// when the host calls `fini`.
pub fn into_ffi_source(source: DaftSourceRef) -> FFI_ScanSource {
    let ctx_ptr = Box::into_raw(Box::new(source));
    FFI_ScanSource {
        ctx: ctx_ptr.cast(),
        name: ffi_name,
        schema: ffi_schema,
        num_tasks: ffi_num_tasks,
        create_task: ffi_create_task,
        next: ffi_next,
        close_task: ffi_close_task,
        fini: ffi_fini,
    }
}

// ---------------------------------------------------------------------------
// Trampolines
// ---------------------------------------------------------------------------

unsafe extern "C" fn ffi_name(ctx: *const c_void) -> *const c_char {
    unsafe { &*ctx.cast::<DaftSourceRef>() }.name().as_ptr()
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_schema(
    ctx:         *const c_void,
    options:     *const c_char,
    options_len: usize,
    ret:         *mut FFI_ArrowSchema,
    errmsg:      *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in source schema", || {
        let ctx = &*ctx.cast::<DaftSourceRef>();
        let options = std::str::from_utf8(std::slice::from_raw_parts(options.cast::<u8>(), options_len))
            .map_err(|e| DaftError::TypeError(format!("invalid UTF-8 options: {e}")))?;
        let fields = ctx.schema(options)?;
        // Build an Arrow schema from the fields and export it.
        let arrow_schema = arrow_schema::Schema::new(fields);
        let ffi_schema = FFI_ArrowSchema::try_from(&arrow_schema)
            .map_err(|e| DaftError::RuntimeError(format!("schema export failed: {e}")))?;
        std::ptr::write(ret, ffi_schema);
        Ok(())
    })}
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_num_tasks(
    ctx:         *const c_void,
    options:     *const c_char,
    options_len: usize,
    errmsg:      *mut *mut c_char,
) -> u32 {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let ctx = unsafe { &*ctx.cast::<DaftSourceRef>() };
        let options = unsafe {
            std::str::from_utf8(std::slice::from_raw_parts(options.cast::<u8>(), options_len))
        };
        match options {
            Ok(options) => ctx.num_tasks(options),
            Err(_) => 0,
        }
    }));
    match result {
        Ok(n) => n,
        Err(_) => {
            unsafe { *errmsg = crate::ffi::strings::new_cstr("panic in source num_tasks".to_string()) };
            0
        }
    }
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_create_task(
    ctx:         *const c_void,
    options:     *const c_char,
    options_len: usize,
    task_index:  u32,
    pushdowns:   *const FFI_Pushdowns,
    ret_task:    *mut *mut c_void,
    errmsg:      *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in source create_task", || {
        let ctx = &*ctx.cast::<DaftSourceRef>();
        let options = std::str::from_utf8(std::slice::from_raw_parts(options.cast::<u8>(), options_len))
            .map_err(|e| DaftError::TypeError(format!("invalid UTF-8 options: {e}")))?;
        let push = &*pushdowns;
        let columns = if push.columns.is_null() {
            None
        } else {
            let mut cols = Vec::with_capacity(push.columns_count);
            for i in 0..push.columns_count {
                let cstr = CStr::from_ptr(*push.columns.add(i));
                cols.push(cstr.to_str()
                    .map_err(|e| DaftError::TypeError(format!("invalid UTF-8 column name: {e}")))?
                    .to_string());
            }
            Some(cols)
        };
        let limit = if push.limit == u64::MAX { None } else { Some(push.limit as usize) };
        let scan_pushdowns = ScanPushdowns { columns, limit };
        let task = ctx.create_task(options, task_index, &scan_pushdowns)?;
        let task_ptr = Box::into_raw(Box::new(task));
        std::ptr::write(ret_task, task_ptr.cast());
        Ok(())
    })}
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_next(
    task:       *mut c_void,
    ret_array:  *mut FFI_ArrowArray,
    ret_schema: *mut FFI_ArrowSchema,
    errmsg:     *mut *mut c_char,
) -> c_int {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let task = unsafe { &mut *task.cast::<Box<dyn DaftSourceTask>>() };
        task.next_batch()
    }));
    match result {
        Ok(Ok(Some(batch))) => {
            let struct_array: arrow_array::StructArray = batch.into();
            let array_ref: arrow_array::ArrayRef = Arc::new(struct_array);
            match arrow::ffi::to_ffi(&array_ref.to_data()) {
                Ok((out_array, out_schema)) => {
                    unsafe {
                        std::ptr::write(ret_array, out_array);
                        std::ptr::write(ret_schema, out_schema);
                    }
                    FFI_SCAN_OK
                }
                Err(e) => {
                    unsafe { *errmsg = crate::ffi::strings::new_cstr(format!("Arrow FFI export failed: {e}")) };
                    FFI_SCAN_ERROR
                }
            }
        }
        Ok(Ok(None)) => FFI_SCAN_DONE,
        Ok(Err(e)) => {
            unsafe { *errmsg = crate::ffi::strings::new_cstr(e.to_string()) };
            FFI_SCAN_ERROR
        }
        Err(_) => {
            unsafe { *errmsg = crate::ffi::strings::new_cstr("panic in source next".to_string()) };
            FFI_SCAN_ERROR
        }
    }
}

unsafe extern "C" fn ffi_close_task(task: *mut c_void) {
    let _ = std::panic::catch_unwind(|| unsafe {
        drop(Box::from_raw(task.cast::<Box<dyn DaftSourceTask>>()));
    });
}

unsafe extern "C" fn ffi_fini(ctx: *mut c_void) {
    let _ = std::panic::catch_unwind(|| unsafe {
        drop(Box::from_raw(ctx.cast::<DaftSourceRef>()));
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    struct MockSource;

    impl DaftSource for MockSource {
        fn name(&self) -> &CStr {
            c"mock"
        }

        fn schema(&self, _options: &str) -> DaftResult<Vec<Field>> {
            Ok(vec![Field::new("value", DataType::Int64, false)])
        }

        fn num_tasks(&self, _options: &str) -> u32 {
            2
        }

        fn create_task(
            &self,
            _options: &str,
            task_index: u32,
            _pushdowns: &ScanPushdowns,
        ) -> DaftResult<Box<dyn DaftSourceTask>> {
            Ok(Box::new(MockTask {
                start: task_index as i64 * 10,
                end: (task_index as i64 + 1) * 10,
            }))
        }
    }

    struct MockTask {
        start: i64,
        end: i64,
    }

    impl DaftSourceTask for MockTask {
        fn next_batch(&mut self) -> DaftResult<Option<RecordBatch>> {
            if self.start >= self.end {
                return Ok(None);
            }
            let values: Int64Array = (self.start..self.end).collect();
            self.start = self.end;
            Ok(Some(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)])),
                    vec![Arc::new(values)],
                )
                .map_err(|e| DaftError::RuntimeError(e.to_string()))?,
            ))
        }
    }

    #[test]
    fn vtable_name_roundtrip() {
        let vtable = into_ffi_source(Arc::new(MockSource));
        let name = unsafe { CStr::from_ptr((vtable.name)(vtable.ctx)) };
        assert_eq!(name.to_str().unwrap(), "mock");
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_num_tasks_roundtrip() {
        let vtable = into_ffi_source(Arc::new(MockSource));
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let n = unsafe { (vtable.num_tasks)(vtable.ctx, c"{}".as_ptr(), 2, &raw mut errmsg) };
        assert_eq!(n, 2);
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }
}
