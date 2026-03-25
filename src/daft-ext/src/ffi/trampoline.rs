use std::{
    ffi::{c_char, c_int},
    panic::AssertUnwindSafe,
};

use super::strings::new_cstr;
use crate::error::DaftResult;

/// Run a fallible closure, catching panics and converting to a C-style
/// return code. On error or panic, writes a message to `*errmsg`.
#[rustfmt::skip]
pub unsafe fn trampoline(
    errmsg: *mut *mut c_char,
    panic_msg: &str,
    f: impl FnOnce() -> DaftResult<()>,
) -> c_int {
    match std::panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => { unsafe { *errmsg = new_cstr(e.to_string()) }; 1 }
        Err(_)     => { unsafe { *errmsg = new_cstr(panic_msg.to_string()) }; 1 }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::{error::DaftError, ffi::strings::free_string};

    #[test]
    fn trampoline_success() {
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { trampoline(&raw mut errmsg, "panic", || Ok(())) };
        assert_eq!(rc, 0);
        assert!(errmsg.is_null());
    }

    #[test]
    fn trampoline_error() {
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            trampoline(&raw mut errmsg, "panic", || {
                Err(DaftError::RuntimeError("test error".into()))
            })
        };
        assert_eq!(rc, 1);
        assert!(!errmsg.is_null());
        let msg = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert!(msg.contains("test error"));
        unsafe { free_string(errmsg) };
    }

    #[test]
    fn trampoline_panic() {
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            trampoline(&raw mut errmsg, "caught a panic", || {
                panic!("oh no");
            })
        };
        assert_eq!(rc, 1);
        assert!(!errmsg.is_null());
        let msg = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert_eq!(msg, "caught a panic");
        unsafe { free_string(errmsg) };
    }
}
