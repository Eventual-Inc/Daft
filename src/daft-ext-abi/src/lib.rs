//! Stable C ABI contract between Daft and extension cdylibs.
//!
//! This crate defines the `repr(C)` types that Daft and extension shared
//! libraries use to communicate. It has zero Daft internal dependencies
//! and zero Arrow implementation dependencies.
//!
//! Naming follows Postgres conventions:
//! - "module" = the shared library at the ABI boundary
//! - "extension" = the higher-level Python package wrapping a module

pub mod arrow;
pub mod ffi;

use std::ffi::{c_char, c_int, c_void};

pub use arrow::{ArrowArray, ArrowArrayStream, ArrowData, ArrowSchema};

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
        args: *const ArrowSchema,
        args_count: usize,
        ret: *mut ArrowSchema,
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
        args: *const ArrowArray,
        args_schemas: *const ArrowSchema,
        args_count: usize,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// Finalize the function, freeing all owned resources.
    pub fini: unsafe extern "C" fn(ctx: *mut c_void),
}

// SAFETY: The vtable is function pointers plus an opaque ctx pointer.
// The module is responsible for thread-safety of ctx.
unsafe impl Send for FFI_ScalarFunction {}
unsafe impl Sync for FFI_ScalarFunction {}

// ---------------------------------------------------------------------------
// Scan source types
// ---------------------------------------------------------------------------

/// Simplified pushdowns passed to scan sources (v1: columns + limit only).
#[repr(C)]
pub struct FFI_Pushdowns {
    /// Projected column names as null-terminated UTF-8 strings, or null for all columns.
    pub columns: *const *const c_char,
    /// Number of entries in `columns`. Zero when `columns` is null.
    pub columns_count: usize,
    /// Row limit. `u64::MAX` means no limit.
    pub limit: u64,
}

// SAFETY: Contains only raw pointers and primitives.
unsafe impl Send for FFI_Pushdowns {}
unsafe impl Sync for FFI_Pushdowns {}

/// Virtual function table for an extension scan source.
///
/// The host calls methods through these function pointers. `ctx` is an opaque
/// pointer owned by the module; the host never dereferences it directly.
///
/// Task reading uses the Arrow C Stream Interface: `create_task` returns an
/// `ArrowArrayStream` that the host consumes via `get_next` / `get_schema`
/// / `release`.
#[repr(C)]
pub struct FFI_ScanSource {
    /// Opaque module-side context pointer.
    pub ctx: *const c_void,

    /// Return the source name as a null-terminated UTF-8 string.
    ///
    /// The returned pointer borrows from `ctx` and is valid until `fini`.
    pub name: unsafe extern "C" fn(ctx: *const c_void) -> *const c_char,

    /// Return the output schema as an Arrow C Data Interface schema.
    ///
    /// `options` is a UTF-8 JSON string of length `options_len`.
    /// On success, writes the schema to `*ret`.
    /// On error, writes a message to `*errmsg` (freed by `FFI_Module::free_string`).
    ///
    /// Returns 0 on success, non-zero on error.
    pub schema: unsafe extern "C" fn(
        ctx: *const c_void,
        options: *const c_char,
        options_len: usize,
        ret: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// Return the number of tasks (partitions) for the given options.
    ///
    /// Returns the task count, or 0 on error (with message in `*errmsg`).
    pub num_tasks: unsafe extern "C" fn(
        ctx: *const c_void,
        options: *const c_char,
        options_len: usize,
        errmsg: *mut *mut c_char,
    ) -> u32,

    /// Create a task (reader) for a partition.
    ///
    /// On success, writes an `ArrowArrayStream` to `*ret_stream`.
    /// The caller owns the stream and must call its `release` when done.
    /// On error, writes a message to `*errmsg`.
    ///
    /// Returns 0 on success, non-zero on error.
    pub create_task: unsafe extern "C" fn(
        ctx: *const c_void,
        options: *const c_char,
        options_len: usize,
        task_index: u32,
        pushdowns: *const FFI_Pushdowns,
        ret_stream: *mut ArrowArrayStream,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// Finalize the source, freeing all owned resources.
    pub fini: unsafe extern "C" fn(ctx: *mut c_void),
}

// SAFETY: The vtable is function pointers plus an opaque ctx pointer.
// The module is responsible for thread-safety of ctx.
unsafe impl Send for FFI_ScanSource {}
unsafe impl Sync for FFI_ScanSource {}

/// Host-side session context passed to a module's `init` function.
///
/// The module calls `define_function` / `define_source` to register extensions.
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

    /// Register a scan source with the host session.
    ///
    /// The host takes ownership of `source` on success.
    /// Returns 0 on success, non-zero on error.
    pub define_source: unsafe extern "C" fn(ctx: *mut c_void, source: FFI_ScanSource) -> c_int,
}

// SAFETY: Function pointer plus opaque host pointer.
unsafe impl Send for FFI_SessionContext {}
unsafe impl Sync for FFI_SessionContext {}

/// Generate arrow-rs conversion helpers using the caller's own arrow version.
///
/// Invoke once in your extension crate to get free functions that convert
/// between arrow-rs types (`ArrayRef`, `Schema`) and the version-agnostic
/// ABI types (`ArrowData`, `ArrowSchema`).
///
/// ```ignore
/// // Uses the `arrow` umbrella crate (most common):
/// daft_ext::define_arrow_helpers!();
///
/// // Or with a custom crate name:
/// daft_ext::define_arrow_helpers!(my_arrow);
/// ```
///
/// Requires the caller to depend on the `arrow` crate (any version) with the
/// `ffi` feature enabled.
#[macro_export]
macro_rules! define_arrow_helpers {
    () => {
        $crate::define_arrow_helpers!(arrow);
    };
    ($arrow_crate:ident) => {
        pub(crate) fn export_array(
            array: &dyn $arrow_crate::array::Array,
        ) -> ::core::result::Result<$crate::ArrowData, ::std::string::String> {
            let (ffi_array, ffi_schema) =
                $arrow_crate::ffi::to_ffi(&array.to_data()).map_err(|e| e.to_string())?;
            let (array, schema) = unsafe { $crate::ffi::arrow::import_ffi(ffi_array, ffi_schema) };
            ::core::result::Result::Ok($crate::ArrowData { schema, array })
        }

        pub(crate) fn import_array(
            data: $crate::ArrowData,
        ) -> ::core::result::Result<$arrow_crate::array::ArrayRef, ::std::string::String> {
            let (ffi_array, ffi_schema): (
                $arrow_crate::ffi::FFI_ArrowArray,
                $arrow_crate::ffi::FFI_ArrowSchema,
            ) = unsafe { $crate::ffi::arrow::export_ffi(data.array, data.schema) };
            let arrow_data = unsafe { $arrow_crate::ffi::from_ffi(ffi_array, &ffi_schema) }
                .map_err(|e| e.to_string())?;
            ::core::result::Result::Ok($arrow_crate::array::make_array(arrow_data))
        }

        pub(crate) fn export_schema(
            schema: &$arrow_crate::datatypes::Schema,
        ) -> ::core::result::Result<$crate::ArrowSchema, ::std::string::String> {
            let ffi_schema =
                $arrow_crate::ffi::FFI_ArrowSchema::try_from(schema).map_err(|e| e.to_string())?;
            ::core::result::Result::Ok(unsafe {
                $crate::ffi::arrow::import_arrow_schema(ffi_schema)
            })
        }

        pub(crate) fn import_schema(
            schema: &$crate::ArrowSchema,
        ) -> ::core::result::Result<$arrow_crate::datatypes::Schema, ::std::string::String> {
            let ffi_schema: &$arrow_crate::ffi::FFI_ArrowSchema =
                unsafe { $crate::ffi::arrow::borrow_arrow_schema(schema) };
            $arrow_crate::datatypes::Schema::try_from(ffi_schema).map_err(|e| e.to_string())
        }

        pub(crate) fn export_field(
            field: &$arrow_crate::datatypes::Field,
        ) -> ::core::result::Result<$crate::ArrowSchema, ::std::string::String> {
            let schema = $arrow_crate::datatypes::Schema::new(vec![field.clone()]);
            export_schema(&schema)
        }

        pub(crate) fn import_field(
            schema: &$crate::ArrowSchema,
        ) -> ::core::result::Result<$arrow_crate::datatypes::Field, ::std::string::String> {
            let s = import_schema(schema)?;
            s.fields()
                .first()
                .cloned()
                .map(|f| (*f).clone())
                .ok_or_else(|| "schema has no fields".to_string())
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_sizes() {
        let ptr = std::mem::size_of::<usize>();

        // FFI_ScalarFunction: ctx + name + get_return_field + call + fini = 5 pointers
        assert_eq!(std::mem::size_of::<FFI_ScalarFunction>(), 5 * ptr);

        // FFI_ScanSource: ctx + name + schema + num_tasks + create_task + fini = 6 pointers
        assert_eq!(std::mem::size_of::<FFI_ScanSource>(), 6 * ptr);

        // FFI_Pushdowns: columns (ptr) + columns_count (usize) + limit (u64)
        assert_eq!(std::mem::size_of::<FFI_Pushdowns>(), 3 * ptr);

        // FFI_SessionContext: ctx + define_function + define_source = 3 pointers
        assert_eq!(std::mem::size_of::<FFI_SessionContext>(), 3 * ptr);

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
        assert_send_sync::<FFI_ScanSource>();
        assert_send_sync::<FFI_Pushdowns>();
        assert_send_sync::<FFI_SessionContext>();
        assert_send_sync::<FFI_Module>();
    }

    #[test]
    fn constants() {
        // !! THIS TEST EXISTS SO THAT THESE ARE NOT CHANGED BY ACCIDENT
        // IT MEANS WE HAVE TO MANUALLY UPDATE IN TWO PLACES !!
        assert_eq!(DAFT_ABI_VERSION, 1);
        assert_eq!(DAFT_MODULE_MAGIC_SYMBOL, "daft_module_magic");
    }
}
