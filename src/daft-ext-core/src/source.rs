use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    sync::Arc,
};

use daft_ext_abi::{ArrowArray, ArrowArrayStream, ArrowData, ArrowSchema, FFI_Pushdowns, FFI_ScanSource};

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

/// Trait for a single scan task that produces Arrow batches.
pub trait DaftSourceTask: Send {
    /// Returns the next batch as C ABI types, or `None` when exhausted.
    fn next_batch(&mut self) -> DaftResult<Option<ArrowData>>;
}

/// Trait that extension authors implement to define a scan source.
pub trait DaftSource: Send + Sync {
    /// Source name (null-terminated, e.g. `c"my_source"`).
    fn name(&self) -> &CStr;

    /// Output schema for the given options, returned as a C ABI ArrowSchema.
    fn schema(&self, options: &str) -> DaftResult<ArrowSchema>;

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
        fini: ffi_fini,
    }
}

// ---------------------------------------------------------------------------
// Source trampolines
// ---------------------------------------------------------------------------

unsafe extern "C" fn ffi_name(ctx: *const c_void) -> *const c_char {
    unsafe { &*ctx.cast::<DaftSourceRef>() }.name().as_ptr()
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_schema(
    ctx:         *const c_void,
    options:     *const c_char,
    options_len: usize,
    ret:         *mut ArrowSchema,
    errmsg:      *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in source schema", || {
        let ctx = &*ctx.cast::<DaftSourceRef>();
        let options = std::str::from_utf8(std::slice::from_raw_parts(options.cast::<u8>(), options_len))
            .map_err(|e| DaftError::TypeError(format!("invalid UTF-8 options: {e}")))?;
        let schema = ctx.schema(options)?;
        std::ptr::write(ret, schema);
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
    ret_stream:  *mut ArrowArrayStream,
    errmsg:      *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in source create_task", || {
        let ctx = &*ctx.cast::<DaftSourceRef>();
        let options_str = std::str::from_utf8(std::slice::from_raw_parts(options.cast::<u8>(), options_len))
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
        let task = ctx.create_task(options_str, task_index, &scan_pushdowns)?;

        // Wrap the task in an ArrowArrayStream.
        let state = Box::new(StreamState {
            source: Arc::clone(ctx),
            options: options_str.to_string(),
            task,
            last_error: None,
        });
        let stream = ArrowArrayStream {
            get_schema: Some(stream_get_schema),
            get_next: Some(stream_get_next),
            get_last_error: Some(stream_get_last_error),
            release: Some(stream_release),
            private_data: Box::into_raw(state).cast(),
        };
        std::ptr::write(ret_stream, stream);
        Ok(())
    })}
}

unsafe extern "C" fn ffi_fini(ctx: *mut c_void) {
    let _ = std::panic::catch_unwind(|| unsafe {
        drop(Box::from_raw(ctx.cast::<DaftSourceRef>()));
    });
}

// ---------------------------------------------------------------------------
// ArrowArrayStream trampolines
// ---------------------------------------------------------------------------

struct StreamState {
    source: DaftSourceRef,
    options: String,
    task: Box<dyn DaftSourceTask>,
    last_error: Option<CString>,
}

unsafe extern "C" fn stream_get_schema(
    stream: *mut ArrowArrayStream,
    out: *mut ArrowSchema,
) -> c_int {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
        state.source.schema(&state.options)
    }));
    match result {
        Ok(Ok(schema)) => {
            unsafe { std::ptr::write(out, schema) };
            0
        }
        Ok(Err(e)) => {
            let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
            state.last_error = CString::new(e.to_string()).ok();
            1
        }
        Err(_) => {
            let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
            state.last_error = CString::new("panic in stream get_schema").ok();
            1
        }
    }
}

unsafe extern "C" fn stream_get_next(stream: *mut ArrowArrayStream, out: *mut ArrowArray) -> c_int {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
        state.task.next_batch()
    }));
    match result {
        Ok(Ok(Some(data))) => {
            // We only write the array to `out`. The schema from ArrowData
            // is dropped (the consumer already has the schema from get_schema).
            unsafe { std::ptr::write(out, data.array) };
            // Release the schema part of ArrowData.
            let mut schema = data.schema;
            if let Some(release) = schema.release {
                unsafe { release(&raw mut schema) };
            }
            0
        }
        Ok(Ok(None)) => {
            // End of stream: write a released array.
            unsafe { std::ptr::write(out, ArrowArray::empty()) };
            0
        }
        Ok(Err(e)) => {
            let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
            state.last_error = CString::new(e.to_string()).ok();
            1
        }
        Err(_) => {
            let state = unsafe { &mut *(*stream).private_data.cast::<StreamState>() };
            state.last_error = CString::new("panic in stream get_next").ok();
            1
        }
    }
}

unsafe extern "C" fn stream_get_last_error(stream: *mut ArrowArrayStream) -> *const c_char {
    let state = unsafe { &*(*stream).private_data.cast::<StreamState>() };
    match &state.last_error {
        Some(s) => s.as_ptr(),
        None => std::ptr::null(),
    }
}

unsafe extern "C" fn stream_release(stream: *mut ArrowArrayStream) {
    let _ = std::panic::catch_unwind(|| unsafe {
        let stream = &mut *stream;
        if !stream.private_data.is_null() {
            drop(Box::from_raw(stream.private_data.cast::<StreamState>()));
            stream.private_data = std::ptr::null_mut();
        }
        stream.get_schema = None;
        stream.get_next = None;
        stream.get_last_error = None;
        stream.release = None;
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::arrow;

    struct MockSource;

    impl DaftSource for MockSource {
        fn name(&self) -> &CStr {
            c"mock"
        }

        fn schema(&self, _options: &str) -> DaftResult<ArrowSchema> {
            let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
            arrow::export_schema(&schema)
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
        fn next_batch(&mut self) -> DaftResult<Option<ArrowData>> {
            if self.start >= self.end {
                return Ok(None);
            }
            let values: Int64Array = (self.start..self.end).collect();
            self.start = self.end;
            let struct_array: arrow_array::StructArray = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(values)],
            )
            .map_err(|e| DaftError::RuntimeError(e.to_string()))?
            .into();
            Ok(Some(arrow::export_array(&struct_array)?))
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

    #[test]
    fn vtable_schema_roundtrip() {
        let vtable = into_ffi_source(Arc::new(MockSource));
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let opts = "{}";
        let rc = unsafe {
            (vtable.schema)(
                vtable.ctx,
                opts.as_ptr().cast(),
                opts.len(),
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        assert!(errmsg.is_null());
        let schema = arrow::import_schema(&ret_schema).unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_create_task_and_read_batches() {
        let vtable = into_ffi_source(Arc::new(MockSource));
        let opts = "{}";
        let pushdowns = FFI_Pushdowns {
            columns: std::ptr::null(),
            columns_count: 0,
            limit: u64::MAX,
        };
        let mut ret_stream = ArrowArrayStream::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.create_task)(
                vtable.ctx,
                opts.as_ptr().cast(),
                opts.len(),
                0,
                &raw const pushdowns,
                &raw mut ret_stream,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        assert!(!ret_stream.is_released());

        // Get schema from the stream.
        let mut stream_schema = ArrowSchema::empty();
        let rc = unsafe {
            (ret_stream.get_schema.unwrap())(&raw mut ret_stream, &raw mut stream_schema)
        };
        assert_eq!(rc, 0);
        let schema = arrow::import_schema(&stream_schema).unwrap();
        assert_eq!(schema.fields().len(), 1);

        // Read the first (and only) batch.
        let mut ret_array = ArrowArray::empty();
        let rc = unsafe { (ret_stream.get_next.unwrap())(&raw mut ret_stream, &raw mut ret_array) };
        assert_eq!(rc, 0);
        assert!(!ret_array.is_released());

        let array_ref = arrow::import_array(ArrowData {
            schema: stream_schema,
            array: ret_array,
        })
        .unwrap();
        let struct_array = array_ref
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .expect("expected struct array");
        let batch = RecordBatch::from(struct_array);
        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 1);

        // Next call should signal end-of-stream (released array).
        let mut ret_array2 = ArrowArray::empty();
        let rc =
            unsafe { (ret_stream.get_next.unwrap())(&raw mut ret_stream, &raw mut ret_array2) };
        assert_eq!(rc, 0);
        assert!(ret_array2.is_released());

        // Release the stream.
        unsafe { (ret_stream.release.unwrap())(&raw mut ret_stream) };
        assert!(ret_stream.is_released());

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_create_task_with_pushdowns() {
        let vtable = into_ffi_source(Arc::new(MockSource));
        let opts = "{}";

        let col_name = std::ffi::CString::new("value").unwrap();
        let col_ptrs = [col_name.as_ptr()];
        let pushdowns = FFI_Pushdowns {
            columns: col_ptrs.as_ptr(),
            columns_count: 1,
            limit: 5,
        };
        let mut ret_stream = ArrowArrayStream::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.create_task)(
                vtable.ctx,
                opts.as_ptr().cast(),
                opts.len(),
                1,
                &raw const pushdowns,
                &raw mut ret_stream,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        assert!(!ret_stream.is_released());

        // Release the stream.
        unsafe { (ret_stream.release.unwrap())(&raw mut ret_stream) };
        assert!(ret_stream.is_released());

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }
}
