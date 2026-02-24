//! Stable C ABI contract between Daft and extension cdylibs.
//!
//! This crate defines the `repr(C)` types that Daft and extension shared
//! libraries use to communicate. It has zero Daft-internal dependencies.

use std::ffi::{c_char, c_int, c_void};

pub use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

/// Daft ABI version; extensions built against a different version are rejected at load time.
pub const DAFT_ABI_VERSION: u32 = 1;

/// Symbol name that every Daft extension cdylib must export.
pub const DAFT_EXTENSION_ENTRY_SYMBOL: &str = "daft_extension_entry";

/// Virtual function table for a scalar function (maps to Daft's `ScalarUDF`).
///
/// The host calls methods through these function pointers. `ctx` is an opaque
/// pointer owned by the extension; the host never dereferences it directly.
///
/// New optional fields may be appended in future ABI versions.
#[repr(C)]
pub struct ExtensionFunction {
    /// Opaque extension-side context pointer e.g. the extension's self reference.
    pub ctx: *const c_void,

    /// Return the function name as a null-terminated UTF-8 string.
    ///
    /// The returned pointer must remain valid until `drop_ctx` is called.
    pub name: unsafe extern "C" fn(ctx: *const c_void) -> *const c_char,

    /// Compute the output field given input fields.
    ///
    /// `args_json` is a null-terminated JSON array of input fields.
    /// On success, writes a null-terminated JSON string to `*out_json`;
    /// the caller frees it with `free_string`.
    ///
    /// Returns 0 on success, non-zero on error.
    pub get_return_field: unsafe extern "C" fn(
        ctx: *const c_void,
        args_json: *const c_char,
        out_json: *mut *mut c_char,
    ) -> c_int,

    /// Evaluate the function on Arrow arrays via the C Data Interface.
    ///
    /// Returns 0 on success, non-zero on error.
    pub call: unsafe extern "C" fn(
        // Opaque context pointer owned by the extension.
        ctx: *const c_void,
        // Pointer to exported `ArrowArray` structs.
        args: *const FFI_ArrowArray,
        // Number of input array args.
        args_count: usize,
        // Pointer to exported `ArrowSchema` structs.
        args_schemas: *const FFI_ArrowSchema,
        // Pointer to the output `ArrowArray` struct.
        out_array: *mut FFI_ArrowArray,
        // Pointer to the output `ArrowSchema` struct.
        out_schema: *mut FFI_ArrowSchema,
    ) -> c_int,

    /// Drop the extension-side context, freeing all owned resources.
    pub drop_ctx: unsafe extern "C" fn(ctx: *mut c_void),

    /// Free a string previously returned by this extension (e.g. from `get_return_field`).
    pub free_string: unsafe extern "C" fn(s: *mut c_char),
}

// SAFETY: The vtable is function pointers plus an opaque ctx pointer.
// The extension is responsible for thread-safety of ctx.
unsafe impl Send for ExtensionFunction {}
unsafe impl Sync for ExtensionFunction {}

/// Host-side context passed to an extension's `install` function.
///
/// The extension calls `create_function` to register scalar functions.
#[repr(C)]
pub struct HostSession {
    /// Opaque host-side context pointer e.g. the host's session reference.
    pub ctx: *mut c_void,

    /// Register a scalar function with the host session.
    ///
    /// The host takes ownership of `function` on success.
    /// Returns 0 on success, non-zero on error.
    pub create_function:
        unsafe extern "C" fn(ctx: *mut c_void, function: ExtensionFunction) -> c_int,
}

// SAFETY: Function pointer plus opaque host pointer.
unsafe impl Send for HostSession {}
unsafe impl Sync for HostSession {}

/// Manifest returned by the extension entry point.
///
/// Every Daft extension cdylib must export:
///
/// ```ignore
/// #[no_mangle]
/// pub extern "C" fn daft_extension_entry() -> ExtManifest { .. }
/// ```
#[repr(C)]
pub struct ExtensionManifest {
    /// Must equal [`DAFT_ABI_VERSION`] or else the loader will reject the extension.
    pub daft_abi_version: u32,

    /// Extension name as a null-terminated UTF-8 string.
    ///
    /// Must remain valid for the lifetime of the process (typically a
    /// `&'static CStr` cast to `*const c_char`).
    pub name: *const c_char,

    /// Called by the host to let the extension register its functions.
    ///
    /// Returns 0 on success, non-zero on error.
    pub install: unsafe extern "C" fn(session: *mut HostSession) -> c_int,
}

// SAFETY: Function pointer plus a static string pointer.
unsafe impl Send for ExtensionManifest {}
unsafe impl Sync for ExtensionManifest {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that repr(C) structs have the expected sizes (pointer-width dependent).
    #[test]
    fn struct_sizes() {
        let ptr = std::mem::size_of::<usize>();

        // FunctionVTable: 1 ctx pointer + 5 function pointers = 6 pointers
        assert_eq!(std::mem::size_of::<ExtensionFunction>(), 6 * ptr);

        // SessionContext: 1 host_ctx pointer + 1 function pointer = 2 pointers
        assert_eq!(std::mem::size_of::<HostSession>(), 2 * ptr);

        // ExtensionManifest: 1 u32 (padded to pointer alignment) + 1 name pointer + 1 fn pointer
        // On 64-bit: 4 bytes + 4 padding + 8 + 8 = 24
        // On 32-bit: 4 bytes + 4 + 4 = 12
        assert_eq!(
            std::mem::size_of::<ExtensionManifest>(),
            if ptr == 8 { 24 } else { 12 }
        );
    }

    /// Confirm Send + Sync are implemented (compile-time check).
    #[test]
    fn send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ExtensionFunction>();
        assert_send_sync::<HostSession>();
        assert_send_sync::<ExtensionManifest>();
    }

    #[test]
    fn constants() {
        assert_eq!(DAFT_ABI_VERSION, 1);
        assert_eq!(DAFT_EXTENSION_ENTRY_SYMBOL, "daft_extension_entry");
    }
}
