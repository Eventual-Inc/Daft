use std::ffi::{CStr, CString, c_char};

use crate::error::{DaftError, DaftResult};

/// Reclaim a C string previously allocated by this crate.
///
/// # Safety
///
/// `s` must have been allocated by `CString::into_raw` in this crate
/// and must not have been freed already.
pub unsafe extern "C" fn free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// Creates a new C string from a string.
pub fn new_cstr(s: String) -> *mut c_char {
    CString::new(s)
        .unwrap_or_else(|_| CString::new("(string contained null byte)").unwrap())
        .into_raw()
}

/// Deserialize `T` from a C JSON string.
pub unsafe fn from_json_cstr<T: serde::de::DeserializeOwned>(ptr: *const c_char) -> DaftResult<T> {
    let json = unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| DaftError::TypeError(format!("invalid UTF-8: {e}")))?;
    serde_json::from_str(json).map_err(|e| DaftError::TypeError(format!("invalid JSON: {e}")))
}

/// Serialize `T` to an owned C string.
pub fn to_json_cstr<T: serde::Serialize>(value: &T) -> DaftResult<*mut c_char> {
    let json = serde_json::to_string(value)
        .map_err(|e| DaftError::RuntimeError(format!("JSON serialization failed: {e}")))?;
    Ok(new_cstr(json))
}
