//! Stable C ABI contract between Daft and extension cdylibs.
//!
//! This crate defines the `repr(C)` types that Daft and extension shared
//! libraries use to communicate. It has zero Daft-internal dependencies.
//!
//! Naming follows Postgres conventions:
//! - "module" = the shared library at the ABI boundary
//! - "extension" = the higher-level Python package wrapping a module

pub mod ffi;

use std::ffi::{c_char, c_int, c_void};

pub use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

/// Modules built against a different ABI version are rejected at load time.
pub const DAFT_ABI_VERSION: u32 = 1;

/// Symbol that every Daft module cdylib must export.
///
/// ```ignore
/// #[no_mangle]
/// pub extern "C" fn daft_module_magic() -> FFI_Module { ... }
/// ```
pub const DAFT_MODULE_MAGIC_SYMBOL: &str = "daft_module_magic";

/// Module definition returned by the entry point symbol.
///
/// Analogous to Postgres's `Pg_magic_struct` + `_PG_init` combined into
/// a single struct.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct FFI_Module {
    /// Must equal [`DAFT_ABI_VERSION`] or the loader rejects the module.
    pub daft_abi_version: u32,

    /// Module name as a null-terminated UTF-8 string.
    ///
    /// Must remain valid for the lifetime of the process (typically a
    /// `&'static CStr` cast to `*const c_char`).
    pub name: *const c_char,

    /// Called by the host to let the module register its functions.
    ///
    /// Returns 0 on success, non-zero on error.
    pub init: unsafe extern "C" fn(session: *mut FFI_SessionContext) -> c_int,

    /// Free a string previously allocated by this module
    /// (e.g. from `FFI_ScalarFunction::get_return_field` or error messages).
    pub free_string: unsafe extern "C" fn(s: *mut c_char),
}

// SAFETY: Function pointers plus a static string pointer.
unsafe impl Send for FFI_Module {}
unsafe impl Sync for FFI_Module {}

/// Virtual function table for a scalar function.
///
/// The host calls methods through these function pointers. `ctx` is an opaque
/// pointer owned by the module; the host never dereferences it directly.
#[repr(C)]
pub struct FFI_ScalarFunction {
    /// Opaque module-side context pointer.
    pub ctx: *const c_void,

    /// Return the function name as a null-terminated UTF-8 string.
    ///
    /// The returned pointer borrows from `ctx` and is valid until `fini`.
    pub name: unsafe extern "C" fn(ctx: *const c_void) -> *const c_char,

    /// Compute the output field given input fields.
    ///
    /// `args` points to `args_count` Arrow field schemas (C Data Interface).
    /// On success, writes the result schema to `*ret`.
    /// On error, writes a null-terminated message to `*errmsg`
    /// (freed by `FFI_Module::free_string`).
    ///
    /// Returns 0 on success, non-zero on error.
    pub get_return_field: unsafe extern "C" fn(
        ctx: *const c_void,
        args: *const FFI_ArrowSchema,
        args_count: usize,
        ret: *mut FFI_ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// Evaluate the function on Arrow arrays via the C Data Interface.
    ///
    /// On error, writes a null-terminated message to `*errmsg`
    /// (freed by `FFI_Module::free_string`).
    ///
    /// Returns 0 on success, non-zero on error.
    pub call: unsafe extern "C" fn(
        ctx: *const c_void,
        args: *const FFI_ArrowArray,
        args_schemas: *const FFI_ArrowSchema,
        args_count: usize,
        ret_array: *mut FFI_ArrowArray,
        ret_schema: *mut FFI_ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// Finalize the function, freeing all owned resources.
    pub fini: unsafe extern "C" fn(ctx: *mut c_void),
}

// SAFETY: The vtable is function pointers plus an opaque ctx pointer.
// The module is responsible for thread-safety of ctx.
unsafe impl Send for FFI_ScalarFunction {}
unsafe impl Sync for FFI_ScalarFunction {}

/// Host-side session context passed to a module's `init` function.
///
/// The module calls `define_function` to register scalar functions.
#[repr(C)]
pub struct FFI_SessionContext {
    /// Opaque host-side context pointer.
    pub ctx: *mut c_void,

    /// Register a scalar function with the host session.
    ///
    /// The host takes ownership of `function` on success.
    /// Returns 0 on success, non-zero on error.
    pub define_function:
        unsafe extern "C" fn(ctx: *mut c_void, function: FFI_ScalarFunction) -> c_int,
}

// SAFETY: Function pointer plus opaque host pointer.
unsafe impl Send for FFI_SessionContext {}
unsafe impl Sync for FFI_SessionContext {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_sizes() {
        let ptr = std::mem::size_of::<usize>();

        // FFI_ScalarFunction: ctx + name + get_return_field + call + fini = 5 pointers
        assert_eq!(std::mem::size_of::<FFI_ScalarFunction>(), 5 * ptr);

        // FFI_SessionContext: ctx + define_function = 2 pointers
        assert_eq!(std::mem::size_of::<FFI_SessionContext>(), 2 * ptr);

        // FFI_Module: u32 (padded) + name + init + free_string
        // 64-bit: 4 + 4 pad + 8 + 8 + 8 = 32
        // 32-bit: 4 + 4 + 4 + 4 = 16
        assert_eq!(
            std::mem::size_of::<FFI_Module>(),
            if ptr == 8 { 32 } else { 16 }
        );
    }

    #[test]
    fn send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FFI_ScalarFunction>();
        assert_send_sync::<FFI_SessionContext>();
        assert_send_sync::<FFI_Module>();
    }

    #[test]
    fn constants() {
        assert_eq!(DAFT_ABI_VERSION, 1);
        assert_eq!(DAFT_MODULE_MAGIC_SYMBOL, "daft_module_magic");
    }
}
