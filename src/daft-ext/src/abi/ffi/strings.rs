use std::ffi::{CString, c_char};

/// Reclaim a C string previously allocated by `CString::into_raw`.
///
/// # Safety
///
/// `s` must have been allocated by `CString::into_raw` in the same allocator
/// and must not have been freed already.
pub unsafe extern "C" fn free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// Creates an owned C string, replacing interior nulls with a placeholder.
pub fn new_cstr(s: String) -> *mut c_char {
    CString::new(s)
        .unwrap_or_else(|_| CString::new("(string contained null byte)").unwrap())
        .into_raw()
}
