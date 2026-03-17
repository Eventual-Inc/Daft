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

    /// Borrow a foreign C Data Interface schema as ours (zero-copy).
    ///
    /// # Safety
    ///
    /// `T` must have the Arrow C Data Interface `ArrowSchema` memory layout.
    pub unsafe fn from_raw<T>(ptr: &T) -> &Self {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowSchema size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowSchema align mismatch"
        );
        unsafe { &*std::ptr::from_ref(ptr).cast::<Self>() }
    }

    /// Mutably borrow a foreign C Data Interface schema as ours (zero-copy).
    ///
    /// # Safety
    ///
    /// `T` must have the Arrow C Data Interface `ArrowSchema` memory layout.
    pub unsafe fn from_raw_mut<T>(ptr: &mut T) -> &mut Self {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowSchema size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowSchema align mismatch"
        );
        unsafe { &mut *std::ptr::from_mut(ptr).cast::<Self>() }
    }

    /// Borrow ours as a foreign C Data Interface schema type (zero-copy).
    ///
    /// # Safety
    ///
    /// `T` must have the Arrow C Data Interface `ArrowSchema` memory layout.
    pub unsafe fn as_raw<T>(&self) -> &T {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowSchema size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowSchema align mismatch"
        );
        unsafe { &*std::ptr::from_ref(self).cast::<T>() }
    }

    /// Take ownership of a foreign C Data Interface schema.
    ///
    /// # Safety
    ///
    /// - `T` must have the Arrow C Data Interface `ArrowSchema` memory layout.
    /// - Ownership transfers — the caller must not use or drop the original.
    pub unsafe fn from_owned<T>(val: T) -> Self {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowSchema size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowSchema align mismatch"
        );
        let val = std::mem::ManuallyDrop::new(val);
        unsafe { std::ptr::read((&raw const *val).cast::<Self>()) }
    }

    /// Convert into a foreign C Data Interface schema type.
    ///
    /// # Safety
    ///
    /// - `T` must have the Arrow C Data Interface `ArrowSchema` memory layout.
    /// - Ownership transfers — the caller must not use or drop the original.
    pub unsafe fn into_owned<T>(self) -> T {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowSchema size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowSchema align mismatch"
        );
        let val = std::mem::ManuallyDrop::new(self);
        unsafe { std::ptr::read((&raw const *val).cast::<T>()) }
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

    /// Borrow a foreign C Data Interface array as ours (zero-copy).
    ///
    /// # Safety
    ///
    /// `T` must have the Arrow C Data Interface `ArrowArray` memory layout.
    pub unsafe fn from_raw<T>(ptr: &T) -> &Self {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowArray size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowArray align mismatch"
        );
        unsafe { &*std::ptr::from_ref(ptr).cast::<Self>() }
    }

    /// Mutably borrow a foreign C Data Interface array as ours (zero-copy).
    ///
    /// # Safety
    ///
    /// `T` must have the Arrow C Data Interface `ArrowArray` memory layout.
    pub unsafe fn from_raw_mut<T>(ptr: &mut T) -> &mut Self {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowArray size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowArray align mismatch"
        );
        unsafe { &mut *std::ptr::from_mut(ptr).cast::<Self>() }
    }

    /// Borrow ours as a foreign C Data Interface array type (zero-copy).
    ///
    /// # Safety
    ///
    /// `T` must have the Arrow C Data Interface `ArrowArray` memory layout.
    pub unsafe fn as_raw<T>(&self) -> &T {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowArray size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowArray align mismatch"
        );
        unsafe { &*std::ptr::from_ref(self).cast::<T>() }
    }

    /// Take ownership of a foreign C Data Interface array.
    ///
    /// # Safety
    ///
    /// - `T` must have the Arrow C Data Interface `ArrowArray` memory layout.
    /// - Ownership transfers — the caller must not use or drop the original.
    pub unsafe fn from_owned<T>(val: T) -> Self {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowArray size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowArray align mismatch"
        );
        let val = std::mem::ManuallyDrop::new(val);
        unsafe { std::ptr::read((&raw const *val).cast::<Self>()) }
    }

    /// Convert into a foreign C Data Interface array type.
    ///
    /// # Safety
    ///
    /// - `T` must have the Arrow C Data Interface `ArrowArray` memory layout.
    /// - Ownership transfers — the caller must not use or drop the original.
    pub unsafe fn into_owned<T>(self) -> T {
        assert_eq!(
            std::mem::size_of::<T>(),
            std::mem::size_of::<Self>(),
            "ArrowArray size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<T>(),
            std::mem::align_of::<Self>(),
            "ArrowArray align mismatch"
        );
        let val = std::mem::ManuallyDrop::new(self);
        unsafe { std::ptr::read((&raw const *val).cast::<T>()) }
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

    /// A mock "foreign" ArrowSchema with identical C layout.
    #[repr(C)]
    struct FakeSchema {
        format: *const c_char,
        name: *const c_char,
        metadata: *const c_char,
        flags: i64,
        n_children: i64,
        children: *mut *mut FakeSchema,
        dictionary: *mut FakeSchema,
        release: Option<unsafe extern "C" fn(schema: *mut FakeSchema)>,
        private_data: *mut c_void,
    }

    /// A mock "foreign" ArrowArray with identical C layout.
    #[repr(C)]
    struct FakeArray {
        length: i64,
        null_count: i64,
        offset: i64,
        n_buffers: i64,
        n_children: i64,
        buffers: *mut *const c_void,
        children: *mut *mut FakeArray,
        dictionary: *mut FakeArray,
        release: Option<unsafe extern "C" fn(array: *mut FakeArray)>,
        private_data: *mut c_void,
    }

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

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn layout_sizes() {
        let ptr = std::mem::size_of::<usize>();
        assert_eq!(std::mem::size_of::<ArrowSchema>(), 9 * ptr);
        assert_eq!(std::mem::size_of::<ArrowArray>(), 10 * ptr);
        assert_eq!(std::mem::size_of::<ArrowArrayStream>(), 5 * ptr);
    }

    #[test]
    fn from_raw_schema() {
        let schema = ArrowSchema::empty();
        let borrowed: &FakeSchema = unsafe { schema.as_raw() };
        assert!(borrowed.release.is_none());

        let fake = FakeSchema {
            format: std::ptr::null(),
            name: std::ptr::null(),
            metadata: std::ptr::null(),
            flags: 42,
            n_children: 0,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        };
        let borrowed: &ArrowSchema = unsafe { ArrowSchema::from_raw(&fake) };
        assert_eq!(borrowed.flags, 42);
    }

    #[test]
    fn from_raw_array() {
        let array = ArrowArray::empty();
        let borrowed: &FakeArray = unsafe { array.as_raw() };
        assert!(borrowed.release.is_none());

        let fake = FakeArray {
            length: 50,
            null_count: 3,
            offset: 0,
            n_buffers: 1,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        };
        let borrowed: &ArrowArray = unsafe { ArrowArray::from_raw(&fake) };
        assert_eq!(borrowed.length, 50);
        assert_eq!(borrowed.null_count, 3);
    }

    #[test]
    fn owned_roundtrip_schema() {
        let original = ArrowSchema::empty();
        let foreign: FakeSchema = unsafe { original.into_owned() };
        let back: ArrowSchema = unsafe { ArrowSchema::from_owned(foreign) };
        assert!(back.is_released());
    }

    #[test]
    fn owned_roundtrip_array() {
        let original = ArrowArray {
            length: 100,
            null_count: 5,
            offset: 10,
            n_buffers: 2,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        };
        let foreign: FakeArray = unsafe { original.into_owned() };
        assert_eq!(foreign.length, 100);
        assert_eq!(foreign.null_count, 5);
        let back: ArrowArray = unsafe { ArrowArray::from_owned(foreign) };
        assert_eq!(back.length, 100);
        assert_eq!(back.null_count, 5);
    }
}
