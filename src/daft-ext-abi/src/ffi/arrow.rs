//! Version-agnostic Arrow C Data Interface conversion helpers.
//!
//! These generic functions convert between our `ArrowArray`/`ArrowSchema`
//! types and *any* foreign type that shares the same `repr(C)` layout
//! (e.g. `arrow::ffi::FFI_ArrowArray` from any arrow-rs version).
//!
//! No arrow-rs dependency is required — correctness relies on the Arrow C
//! Data Interface specification guaranteeing a stable memory layout.

use crate::{ArrowArray, ArrowSchema};

/// Convert a foreign Arrow C Data Interface struct pair into ours.
///
/// Both `arrow::ffi::FFI_ArrowArray` (from any arrow-rs version) and our
/// `ArrowArray` have the same `repr(C)` layout as defined by the Arrow C
/// Data Interface spec. This function performs a pointer-cast read and
/// prevents the source from running its `Drop`/release.
///
/// # Safety
///
/// - `A` must have the Arrow C Data Interface `ArrowArray` memory layout.
/// - `S` must have the Arrow C Data Interface `ArrowSchema` memory layout.
/// - Ownership transfers — the caller must not use or drop the originals.
pub unsafe fn import_ffi<A, S>(array: A, schema: S) -> (ArrowArray, ArrowSchema) {
    assert_eq!(
        std::mem::size_of::<A>(),
        std::mem::size_of::<ArrowArray>(),
        "foreign ArrowArray size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<A>(),
        std::mem::align_of::<ArrowArray>(),
        "foreign ArrowArray alignment mismatch"
    );
    assert_eq!(
        std::mem::size_of::<S>(),
        std::mem::size_of::<ArrowSchema>(),
        "foreign ArrowSchema size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<S>(),
        std::mem::align_of::<ArrowSchema>(),
        "foreign ArrowSchema alignment mismatch"
    );
    let arr = unsafe { std::ptr::read((&raw const array).cast::<ArrowArray>()) };
    let sch = unsafe { std::ptr::read((&raw const schema).cast::<ArrowSchema>()) };
    std::mem::forget(array);
    std::mem::forget(schema);
    (arr, sch)
}

/// Convert our `ArrowArray`/`ArrowSchema` pair into foreign types.
///
/// The inverse of [`import_ffi`]. Reads our structs as the target types
/// and prevents our `Drop` from running.
///
/// # Safety
///
/// - `A` must have the Arrow C Data Interface `ArrowArray` memory layout.
/// - `S` must have the Arrow C Data Interface `ArrowSchema` memory layout.
/// - Ownership transfers — the caller must not use or drop the originals.
pub unsafe fn export_ffi<A, S>(array: ArrowArray, schema: ArrowSchema) -> (A, S) {
    assert_eq!(
        std::mem::size_of::<A>(),
        std::mem::size_of::<ArrowArray>(),
        "foreign ArrowArray size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<A>(),
        std::mem::align_of::<ArrowArray>(),
        "foreign ArrowArray alignment mismatch"
    );
    assert_eq!(
        std::mem::size_of::<S>(),
        std::mem::size_of::<ArrowSchema>(),
        "foreign ArrowSchema size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<S>(),
        std::mem::align_of::<ArrowSchema>(),
        "foreign ArrowSchema alignment mismatch"
    );
    let array = std::mem::ManuallyDrop::new(array);
    let schema = std::mem::ManuallyDrop::new(schema);
    let a = unsafe { std::ptr::read((&raw const *array).cast::<A>()) };
    let s = unsafe { std::ptr::read((&raw const *schema).cast::<S>()) };
    (a, s)
}

// ---------------------------------------------------------------------------
// Single-value array helpers
// ---------------------------------------------------------------------------

/// Import a single foreign array into ours.
///
/// # Safety
///
/// `A` must have the Arrow C Data Interface `ArrowArray` memory layout.
/// Ownership transfers.
pub unsafe fn import_arrow_array<A>(array: A) -> ArrowArray {
    assert_eq!(
        std::mem::size_of::<A>(),
        std::mem::size_of::<ArrowArray>(),
        "foreign ArrowArray size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<A>(),
        std::mem::align_of::<ArrowArray>(),
        "foreign ArrowArray alignment mismatch"
    );
    let array = std::mem::ManuallyDrop::new(array);
    unsafe { std::ptr::read((&raw const *array).cast::<ArrowArray>()) }
}

/// Export our `ArrowArray` into a foreign array type.
///
/// # Safety
///
/// `A` must have the Arrow C Data Interface `ArrowArray` memory layout.
/// Ownership transfers.
pub unsafe fn export_arrow_array<A>(array: ArrowArray) -> A {
    assert_eq!(
        std::mem::size_of::<A>(),
        std::mem::size_of::<ArrowArray>(),
        "foreign ArrowArray size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<A>(),
        std::mem::align_of::<ArrowArray>(),
        "foreign ArrowArray alignment mismatch"
    );
    let array = std::mem::ManuallyDrop::new(array);
    unsafe { std::ptr::read((&raw const *array).cast::<A>()) }
}

// ---------------------------------------------------------------------------
// Single-value schema helpers
// ---------------------------------------------------------------------------

/// Import a single foreign schema into ours.
///
/// # Safety
///
/// `S` must have the Arrow C Data Interface `ArrowSchema` memory layout.
/// Ownership transfers.
pub unsafe fn import_arrow_schema<S>(schema: S) -> ArrowSchema {
    assert_eq!(
        std::mem::size_of::<S>(),
        std::mem::size_of::<ArrowSchema>(),
        "foreign ArrowSchema size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<S>(),
        std::mem::align_of::<ArrowSchema>(),
        "foreign ArrowSchema alignment mismatch"
    );
    let schema = std::mem::ManuallyDrop::new(schema);
    unsafe { std::ptr::read((&raw const *schema).cast::<ArrowSchema>()) }
}

/// Export our `ArrowSchema` into a foreign schema type.
///
/// # Safety
///
/// `S` must have the Arrow C Data Interface `ArrowSchema` memory layout.
/// Ownership transfers.
pub unsafe fn export_arrow_schema<S>(schema: ArrowSchema) -> S {
    assert_eq!(
        std::mem::size_of::<S>(),
        std::mem::size_of::<ArrowSchema>(),
        "foreign ArrowSchema size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<S>(),
        std::mem::align_of::<ArrowSchema>(),
        "foreign ArrowSchema alignment mismatch"
    );
    let schema = std::mem::ManuallyDrop::new(schema);
    unsafe { std::ptr::read((&raw const *schema).cast::<S>()) }
}

// ---------------------------------------------------------------------------
// Borrow helpers (zero-copy casts, no ownership transfer)
// ---------------------------------------------------------------------------

/// Borrow our `ArrowSchema` as a reference to a foreign schema type.
///
/// This is a zero-copy cast — no ownership transfer occurs.
///
/// # Safety
///
/// - `S` must have the Arrow C Data Interface `ArrowSchema` memory layout.
/// - `S` must be a plain `#[repr(C)]` struct with no `Drop` impl.
pub unsafe fn borrow_arrow_schema<S>(schema: &ArrowSchema) -> &S {
    assert_eq!(
        std::mem::size_of::<S>(),
        std::mem::size_of::<ArrowSchema>(),
        "foreign ArrowSchema size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<S>(),
        std::mem::align_of::<ArrowSchema>(),
        "foreign ArrowSchema alignment mismatch"
    );
    unsafe { &*std::ptr::from_ref::<ArrowSchema>(schema).cast::<S>() }
}

/// Borrow our `ArrowArray` as a reference to a foreign array type.
///
/// This is a zero-copy cast — no ownership transfer occurs.
///
/// # Safety
///
/// - `A` must have the Arrow C Data Interface `ArrowArray` memory layout.
/// - `A` must be a plain `#[repr(C)]` struct with no `Drop` impl.
pub unsafe fn borrow_arrow_array<A>(array: &ArrowArray) -> &A {
    assert_eq!(
        std::mem::size_of::<A>(),
        std::mem::size_of::<ArrowArray>(),
        "foreign ArrowArray size mismatch"
    );
    assert_eq!(
        std::mem::align_of::<A>(),
        std::mem::align_of::<ArrowArray>(),
        "foreign ArrowArray alignment mismatch"
    );
    unsafe { &*std::ptr::from_ref::<ArrowArray>(array).cast::<A>() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mock "foreign" ArrowSchema with identical C layout.
    #[repr(C)]
    struct FakeSchema {
        format: *const std::ffi::c_char,
        name: *const std::ffi::c_char,
        metadata: *const std::ffi::c_char,
        flags: i64,
        n_children: i64,
        children: *mut *mut FakeSchema,
        dictionary: *mut FakeSchema,
        release: Option<unsafe extern "C" fn(schema: *mut FakeSchema)>,
        private_data: *mut std::ffi::c_void,
    }

    /// A mock "foreign" ArrowArray with identical C layout.
    #[repr(C)]
    struct FakeArray {
        length: i64,
        null_count: i64,
        offset: i64,
        n_buffers: i64,
        n_children: i64,
        buffers: *mut *const std::ffi::c_void,
        children: *mut *mut FakeArray,
        dictionary: *mut FakeArray,
        release: Option<unsafe extern "C" fn(array: *mut FakeArray)>,
        private_data: *mut std::ffi::c_void,
    }

    #[test]
    fn round_trip_schema() {
        let original = ArrowSchema::empty();
        let foreign: FakeSchema = unsafe { export_arrow_schema(original) };
        let back: ArrowSchema = unsafe { import_arrow_schema(foreign) };
        assert!(back.is_released());
    }

    #[test]
    fn round_trip_pair() {
        let arr = ArrowArray::empty();
        let sch = ArrowSchema::empty();
        let (fa, fs): (FakeArray, FakeSchema) = unsafe { export_ffi(arr, sch) };
        let (back_arr, back_sch) = unsafe { import_ffi(fa, fs) };
        assert!(back_arr.is_released());
        assert!(back_sch.is_released());
    }

    #[test]
    fn borrow_schema() {
        let schema = ArrowSchema::empty();
        let borrowed: &FakeSchema = unsafe { borrow_arrow_schema(&schema) };
        assert!(borrowed.release.is_none());
    }

    #[test]
    fn borrow_array() {
        let array = ArrowArray::empty();
        let borrowed: &FakeArray = unsafe { borrow_arrow_array(&array) };
        assert!(borrowed.release.is_none());
        assert_eq!(borrowed.length, 0);
    }

    unsafe extern "C" fn fake_schema_release(_: *mut FakeSchema) {}
    unsafe extern "C" fn fake_array_release(_: *mut FakeArray) {}

    #[test]
    fn round_trip_schema_populated() {
        let format = c"i";
        let name = c"test_field";
        let original = ArrowSchema {
            format: format.as_ptr(),
            name: name.as_ptr(),
            metadata: std::ptr::null(),
            flags: 42,
            n_children: 0,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(unsafe {
                std::mem::transmute::<
                    unsafe extern "C" fn(*mut FakeSchema),
                    unsafe extern "C" fn(*mut ArrowSchema),
                >(fake_schema_release)
            }),
            private_data: std::ptr::null_mut(),
        };
        let foreign: FakeSchema = unsafe { export_arrow_schema(original) };
        assert_eq!(foreign.flags, 42);
        assert!(!foreign.format.is_null());
        assert!(foreign.release.is_some());

        let back: ArrowSchema = unsafe { import_arrow_schema(foreign) };
        assert_eq!(back.flags, 42);
        assert!(!back.is_released());
        assert_eq!(unsafe { std::ffi::CStr::from_ptr(back.format) }, c"i");
    }

    #[test]
    fn round_trip_array_populated() {
        let original = ArrowArray {
            length: 100,
            null_count: 5,
            offset: 10,
            n_buffers: 2,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(unsafe {
                std::mem::transmute::<
                    unsafe extern "C" fn(*mut FakeArray),
                    unsafe extern "C" fn(*mut ArrowArray),
                >(fake_array_release)
            }),
            private_data: std::ptr::null_mut(),
        };
        let foreign: FakeArray = unsafe { export_arrow_array(original) };
        assert_eq!(foreign.length, 100);
        assert_eq!(foreign.null_count, 5);
        assert_eq!(foreign.offset, 10);
        assert_eq!(foreign.n_buffers, 2);
        assert!(foreign.release.is_some());

        let back: ArrowArray = unsafe { import_arrow_array(foreign) };
        assert_eq!(back.length, 100);
        assert_eq!(back.null_count, 5);
        assert!(!back.is_released());
    }

    #[test]
    fn borrow_schema_populated() {
        let format = c"d";
        let schema = ArrowSchema {
            format: format.as_ptr(),
            name: std::ptr::null(),
            metadata: std::ptr::null(),
            flags: 99,
            n_children: 0,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        };
        let borrowed: &FakeSchema = unsafe { borrow_arrow_schema(&schema) };
        assert_eq!(borrowed.flags, 99);
        assert_eq!(unsafe { std::ffi::CStr::from_ptr(borrowed.format) }, c"d");
    }

    #[test]
    fn borrow_array_populated() {
        let array = ArrowArray {
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
        let borrowed: &FakeArray = unsafe { borrow_arrow_array(&array) };
        assert_eq!(borrowed.length, 50);
        assert_eq!(borrowed.null_count, 3);
        assert_eq!(borrowed.n_buffers, 1);
    }
}
