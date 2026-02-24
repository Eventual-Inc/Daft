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
