//! Arrow C Data Interface and C Stream Interface types.
//!
//! These are `#[repr(C)]` definitions matching the official Arrow specs:
//! - <https://arrow.apache.org/docs/format/CDataInterface.html>
//! - <https://arrow.apache.org/docs/format/CStreamInterface.html>
//!
//! By owning these types, `daft-ext-abi` has **zero** dependency on any
//! Arrow Rust implementation (arrow-rs, arrow2, etc.).

use std::ffi::{c_char, c_int, c_void};

/// An Arrow array paired with its schema (C Data Interface).
///
/// Note: This is a convenience type rather than passing (ArrowArray, ArrowSchema).
#[repr(C)]
pub struct ArrowData {
    pub schema: ArrowSchema,
    pub array: ArrowArray,
}

impl ArrowData {
    /// Take an `ArrowData` from a `&[ArrowData]` slice by index, zeroing the
    /// source slot so the original cannot be accidentally reused.
    ///
    /// This is the safe way to consume arguments in `DaftScalarFunction::call`
    /// instead of raw `std::ptr::read`.
    ///
    /// # Safety
    ///
    /// - `index` must be in bounds.
    /// - Each index must be taken at most once (the slot is zeroed after taking).
    pub unsafe fn take_arg(args: &[Self], index: usize) -> Self {
        let ptr = unsafe { args.as_ptr().add(index).cast_mut() };
        let data = unsafe { std::ptr::read(ptr) };
        unsafe { std::ptr::write_bytes(ptr, 0, 1) };
        data
    }
}

/// ArrowSchema C Data Interface.
///
/// See: <https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure>
///
/// **Ownership:** This type has no `Drop` impl. Callers must invoke the
/// `release` callback (if `Some`) before dropping to free resources.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(schema: *mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

impl ArrowSchema {
    /// Create an empty (released) schema.
    pub fn empty() -> Self {
        Self {
            format: std::ptr::null(),
            name: std::ptr::null(),
            metadata: std::ptr::null(),
            flags: 0,
            n_children: 0,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// Whether this schema has been released (release callback is None).
    pub fn is_released(&self) -> bool {
        self.release.is_none()
    }
}

// SAFETY: ArrowSchema is a plain C struct with raw pointers.
// Only Send — concurrent &-access isn't safe per the Arrow C Data Interface spec.
unsafe impl Send for ArrowSchema {}

/// ArrowArray C Data Interface: array (columnar data).
///
/// See: <https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure>
///
/// **Ownership:** This type has no `Drop` impl. Callers must invoke the
/// `release` callback (if `Some`) before dropping to free resources.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(array: *mut ArrowArray)>,
    pub private_data: *mut c_void,
}

impl ArrowArray {
    /// Create an empty (released) array.
    pub fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// Whether this array has been released (release callback is None).
    pub fn is_released(&self) -> bool {
        self.release.is_none()
    }
}

// SAFETY: ArrowArray is a plain C struct with raw pointers.
// Only Send — concurrent &-access isn't safe per the Arrow C Data Interface spec.
unsafe impl Send for ArrowArray {}

/// ArrowArray C Stream Interface is a streaming producer of Arrow record batches.
///
/// <https://arrow.apache.org/docs/format/CStreamInterface.html#the-arrowarraystream-structure>
///
/// **Ownership:** This type has no `Drop` impl. Callers must invoke the
/// `release` callback (if `Some`) before dropping to free resources.
#[repr(C)]
pub struct ArrowArrayStream {
    /// Get the schema of the stream.
    ///
    /// On success, writes to `*out` and returns 0.
    /// On error, returns non-zero; caller may call `get_last_error`.
    pub get_schema:
        Option<unsafe extern "C" fn(stream: *mut ArrowArrayStream, out: *mut ArrowSchema) -> c_int>,

    /// Get the next record batch.
    ///
    /// On success, writes to `*out` and returns 0.
    /// End-of-stream is signaled by writing a released array (release == None).
    /// On error, returns non-zero; caller may call `get_last_error`.
    pub get_next:
        Option<unsafe extern "C" fn(stream: *mut ArrowArrayStream, out: *mut ArrowArray) -> c_int>,

    /// Get a human-readable error message for the last error.
    ///
    /// Returns a pointer to a null-terminated string, or null if no error.
    /// The pointer is valid until the next call on this stream or until release.
    pub get_last_error:
        Option<unsafe extern "C" fn(stream: *mut ArrowArrayStream) -> *const c_char>,

    /// Release the stream and all associated resources.
    ///
    /// After calling, the stream is in a released state (all pointers None/null).
    pub release: Option<unsafe extern "C" fn(stream: *mut ArrowArrayStream)>,

    /// Opaque producer-specific data.
    pub private_data: *mut c_void,
}

impl ArrowArrayStream {
    /// Create an empty (released) stream.
    pub fn empty() -> Self {
        Self {
            get_schema: None,
            get_next: None,
            get_last_error: None,
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// Whether this stream has been released (release callback is None).
    pub fn is_released(&self) -> bool {
        self.release.is_none()
    }
}

// SAFETY: ArrowArrayStream is a plain C struct with function pointers + opaque data.
// Only Send — concurrent &-access isn't safe per the Arrow C Data Interface spec.
unsafe impl Send for ArrowArrayStream {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_schema_empty() {
        let s = ArrowSchema::empty();
        assert!(s.is_released());
        assert!(s.format.is_null());
        assert!(s.name.is_null());
    }

    #[test]
    fn arrow_array_empty() {
        let a = ArrowArray::empty();
        assert!(a.is_released());
        assert_eq!(a.length, 0);
    }

    #[test]
    fn arrow_array_stream_empty() {
        let s = ArrowArrayStream::empty();
        assert!(s.is_released());
        assert!(s.private_data.is_null());
    }

    #[test]
    fn send_only() {
        fn assert_send<T: Send>() {}
        assert_send::<ArrowSchema>();
        assert_send::<ArrowArray>();
        assert_send::<ArrowArrayStream>();
    }

    #[test]
    fn layout_sizes() {
        let ptr = std::mem::size_of::<usize>();

        // ArrowSchema: 9 fields (format, name, metadata: *const; flags: i64;
        // n_children: i64; children, dictionary: *mut; release: Option<fn>;
        // private_data: *mut) = 9 pointer-sized on 64-bit
        assert_eq!(std::mem::size_of::<ArrowSchema>(), 9 * ptr);

        // ArrowArray: 10 fields (length, null_count, offset: i64;
        // n_buffers, n_children: i64; buffers, children, dictionary: *mut;
        // release: Option<fn>; private_data: *mut) = 10 pointer-sized on 64-bit
        assert_eq!(std::mem::size_of::<ArrowArray>(), 10 * ptr);

        // ArrowArrayStream: 5 fields (get_schema, get_next, get_last_error,
        // release: Option<fn>; private_data: *mut) = 5 pointer-sized
        assert_eq!(std::mem::size_of::<ArrowArrayStream>(), 5 * ptr);
    }
}
