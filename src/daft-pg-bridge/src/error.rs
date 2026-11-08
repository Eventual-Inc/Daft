//! Error handling for C ABI
//!
//! All exported C functions must catch panics and convert errors to stable status codes.

use std::{
    ffi::{CStr, CString},
    os::raw::c_char,
    ptr,
};

/// Stable error codes for C ABI
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpgErrorCode {
    /// Operation succeeded
    Success = 0,
    /// Invalid argument
    InvalidArg = 1,
    /// Invalid state
    InvalidState = 2,
    /// Resource exhausted
    ResourceExhausted = 3,
    /// Operation cancelled
    Cancelled = 4,
    /// Internal error
    InternalError = 5,
    /// Panic occurred
    Panic = 6,
}

/// Opaque error object for C ABI
#[repr(C)]
#[derive(Debug)]
pub struct DpgError {
    code: DpgErrorCode,
    message: *mut c_char,
}

impl DpgError {
    /// Create a new error with a message
    pub fn new(code: DpgErrorCode, message: String) -> Box<Self> {
        // Sanitize error message (remove sensitive info)
        let sanitized = Self::sanitize_message(&message);

        let c_message = CString::new(sanitized)
            .unwrap_or_else(|_| CString::new("Invalid UTF-8 in error message").unwrap());

        Box::new(Self {
            code,
            message: c_message.into_raw(),
        })
    }

    /// Sanitize error message to remove sensitive information
    fn sanitize_message(msg: &str) -> String {
        let lower = msg.to_ascii_lowercase();
        const PATH_MARKERS: &[&str] = &[
            "/home/",
            "/users/",
            "c:\\users\\",
            "/tmp/pgdaft-",
            "/var/tmp/pgdaft-",
        ];
        if PATH_MARKERS.iter().any(|marker| lower.contains(marker)) {
            return "PGDAFT_ERROR_PATH_REDACTED <HOME>".to_string();
        }
        const SENSITIVE_MARKERS: &[&str] = &[
            "password=",
            "passwd=",
            "token=",
            "secret=",
            "credential",
            "authorization:",
            "bearer ",
            "postgres://",
            "postgresql://",
            "s3://",
            "http://",
            "https://",
        ];
        if SENSITIVE_MARKERS
            .iter()
            .any(|marker| lower.contains(marker))
        {
            return "PGDAFT_ERROR_DETAIL_REDACTED".to_string();
        }

        // Truncate on a UTF-8 character boundary.  Byte slicing at offset 512
        // can itself panic while constructing the error that is meant to
        // contain a panic crossing the ABI.
        const MAX_BYTES: usize = 512;
        if msg.len() <= MAX_BYTES {
            return msg.to_string();
        }
        let boundary = msg
            .char_indices()
            .map(|(index, _)| index)
            .take_while(|index| *index <= MAX_BYTES)
            .last()
            .unwrap_or(0);
        format!("{}... (truncated)", &msg[..boundary])
    }

    /// Get error code
    pub fn code(&self) -> DpgErrorCode {
        self.code
    }

    /// Get error message (borrowed)
    pub fn message(&self) -> &str {
        if self.message.is_null() {
            ""
        } else {
            unsafe {
                CStr::from_ptr(self.message)
                    .to_str()
                    .unwrap_or("<invalid UTF-8>")
            }
        }
    }
}

impl Drop for DpgError {
    fn drop(&mut self) {
        if !self.message.is_null() {
            unsafe {
                let _ = CString::from_raw(self.message);
            }
        }
    }
}

/// Free an error object (idempotent)
///
/// # Safety
/// - `error` must be a valid pointer returned by an ABI function, or NULL
/// - After this call, `*error` will be set to NULL
#[no_mangle]
pub unsafe extern "C" fn dpg_error_free(error: *mut *mut DpgError) {
    if !error.is_null() && !(*error).is_null() {
        let _ = Box::from_raw(*error);
        *error = ptr::null_mut();
    }
}

/// Get error code
///
/// # Safety
/// - `error` must be a valid pointer or NULL
#[no_mangle]
pub unsafe extern "C" fn dpg_error_code(error: *const DpgError) -> DpgErrorCode {
    if error.is_null() {
        DpgErrorCode::Success
    } else {
        (*error).code()
    }
}

/// Get error message
///
/// # Safety
/// - `error` must be a valid pointer or NULL
/// - Returned pointer is valid until error is freed
#[no_mangle]
pub unsafe extern "C" fn dpg_error_message(error: *const DpgError) -> *const c_char {
    if error.is_null() {
        ptr::null()
    } else {
        (*error).message
    }
}

/// Helper macro to catch panics and convert to C errors
#[macro_export]
macro_rules! catch_panic {
    ($error_out:expr, $code:expr) => {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| $code)) {
            Ok(result) => result,
            Err(panic_info) => {
                let _ = panic_info;
                let message = "PGDAFT_PANIC: internal panic contained at ABI boundary".to_string();

                if !$error_out.is_null() {
                    *$error_out = Box::into_raw($crate::error::DpgError::new(
                        $crate::error::DpgErrorCode::Panic,
                        message,
                    ));
                }

                return $crate::error::DpgErrorCode::Panic;
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let error = DpgError::new(DpgErrorCode::InvalidArg, "test error".to_string());
        assert_eq!(error.code(), DpgErrorCode::InvalidArg);
        assert_eq!(error.message(), "test error");
    }

    #[test]
    fn test_error_sanitization() {
        let error = DpgError::new(
            DpgErrorCode::InternalError,
            "/home/user/secret/file.txt".to_string(),
        );
        assert_eq!(error.message(), "PGDAFT_ERROR_PATH_REDACTED <HOME>");
        assert!(!error.message().contains("/home/"));
    }

    #[test]
    fn test_error_free_null() {
        unsafe {
            let mut error: *mut DpgError = ptr::null_mut();
            dpg_error_free(&mut error);
            assert!(error.is_null());
        }
    }

    #[test]
    fn test_error_free_idempotent() {
        let error = DpgError::new(DpgErrorCode::Success, "test".to_string());
        let mut error_ptr = Box::into_raw(error);

        unsafe {
            dpg_error_free(&mut error_ptr);
            assert!(error_ptr.is_null());

            // Should be idempotent
            dpg_error_free(&mut error_ptr);
            assert!(error_ptr.is_null());
        }
    }

    #[test]
    fn test_error_message_truncation() {
        let long_msg = "a".repeat(1000);
        let error = DpgError::new(DpgErrorCode::InternalError, long_msg);
        let msg = error.message();
        assert!(msg.len() <= 512 + 20); // 512 + "... (truncated)"
        assert!(msg.contains("(truncated)"));
    }

    #[test]
    fn test_multibyte_error_message_truncation_is_panic_free() {
        let error = DpgError::new(DpgErrorCode::InternalError, "数".repeat(400));
        assert!(error.message().is_char_boundary(error.message().len()));
        assert!(error.message().contains("(truncated)"));
    }
}
