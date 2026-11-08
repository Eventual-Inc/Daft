//! ABI v1 tests for panic handling and lifecycle

use std::ptr;

use daft_pg_bridge::{
    catch_panic,
    error::{dpg_error_code, dpg_error_free, dpg_error_message, DpgError, DpgErrorCode},
};

#[test]
fn test_panic_becomes_error() {
    // Simulate a function that panics
    unsafe fn panicking_function(error_out: *mut *mut DpgError) -> DpgErrorCode {
        catch_panic!(error_out, {
            panic!("Test panic");
            #[allow(unreachable_code)]
            DpgErrorCode::Success
        })
    }

    let mut error: *mut DpgError = ptr::null_mut();
    let code = unsafe { panicking_function(&mut error) };

    assert_eq!(code, DpgErrorCode::Panic);
    assert!(!error.is_null());

    unsafe {
        assert_eq!(dpg_error_code(error), DpgErrorCode::Panic);
        let msg = dpg_error_message(error);
        assert!(!msg.is_null());

        dpg_error_free(&mut error);
        assert!(error.is_null());
    }
}

#[test]
fn test_error_free_sets_null() {
    let error = DpgError::new(DpgErrorCode::InvalidArg, "test".to_string());
    let mut error_ptr = Box::into_raw(error);

    unsafe {
        assert!(!error_ptr.is_null());
        dpg_error_free(&mut error_ptr);
        assert!(error_ptr.is_null());
    }
}

#[test]
fn test_error_free_idempotent() {
    let error = DpgError::new(DpgErrorCode::Success, "test".to_string());
    let mut error_ptr = Box::into_raw(error);

    unsafe {
        dpg_error_free(&mut error_ptr);
        assert!(error_ptr.is_null());

        // Second call should be safe
        dpg_error_free(&mut error_ptr);
        assert!(error_ptr.is_null());

        // Third call should also be safe
        dpg_error_free(&mut error_ptr);
        assert!(error_ptr.is_null());
    }
}

#[test]
fn test_error_message_sanitized() {
    let error = DpgError::new(
        DpgErrorCode::InternalError,
        "Error at /home/user/secret.rs:42".to_string(),
    );

    let msg = error.message();
    assert!(msg.contains("<HOME>"));
    assert!(!msg.contains("/home/"));
}

#[test]
fn test_null_error_returns_success() {
    unsafe {
        let code = dpg_error_code(ptr::null());
        assert_eq!(code, DpgErrorCode::Success);

        let msg = dpg_error_message(ptr::null());
        assert!(msg.is_null());
    }
}

#[test]
fn test_error_lifecycle() {
    // Create error
    let error = DpgError::new(DpgErrorCode::ResourceExhausted, "Queue full".to_string());
    let mut error_ptr = Box::into_raw(error);

    unsafe {
        // Check code
        assert_eq!(dpg_error_code(error_ptr), DpgErrorCode::ResourceExhausted);

        // Check message
        let msg = dpg_error_message(error_ptr);
        assert!(!msg.is_null());

        // Free
        dpg_error_free(&mut error_ptr);
        assert!(error_ptr.is_null());

        // After free, should be idempotent
        dpg_error_free(&mut error_ptr);
    }
}

#[test]
fn test_multiple_errors() {
    let error1 = DpgError::new(DpgErrorCode::InvalidArg, "error1".to_string());
    let error2 = DpgError::new(DpgErrorCode::InvalidState, "error2".to_string());

    let mut ptr1 = Box::into_raw(error1);
    let mut ptr2 = Box::into_raw(error2);

    unsafe {
        assert_eq!(dpg_error_code(ptr1), DpgErrorCode::InvalidArg);
        assert_eq!(dpg_error_code(ptr2), DpgErrorCode::InvalidState);

        dpg_error_free(&mut ptr1);
        dpg_error_free(&mut ptr2);

        assert!(ptr1.is_null());
        assert!(ptr2.is_null());
    }
}
