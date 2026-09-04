//! Stable C ABI contract between Daft and extension cdylibs.
//!
//! This module defines the `repr(C)` types that Daft and extension shared
//! libraries use to communicate. It has zero Daft internal dependencies
//! and zero Arrow implementation dependencies (unless a feature flag is enabled).
//!
//! Naming follows Postgres conventions:
//! - "module" = the shared library at the ABI boundary
//! - "extension" = the higher-level Python package wrapping a module

pub mod arrow;
pub mod compat;
pub mod ffi;

use std::ffi::{c_char, c_int, c_void};

pub use arrow::{ArrowArray, ArrowArrayStream, ArrowData, ArrowSchema};

/// Modules built against a different ABI version are rejected at load time.
///
/// History:
/// - `1` — initial release.
/// - `2` — `FFI_ScalarFunction::get_return_field` takes [`ArgDescriptor`]s
///   instead of bare [`ArrowSchema`]s.
pub const DAFT_ABI_VERSION: u32 = 2;

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

/// Planning-time description of a single argument to a scalar function.
///
/// Carries the argument's field, plus its *value* when the argument is a
/// literal at the point the call is planned. Every other argument — a column, or
/// any expression, including one that is constant but not yet folded — carries a
/// released `literal`.
///
/// **Ownership:** the host owns both members for the duration of the
/// `get_return_field` call and releases them afterwards. The module borrows
/// them; it must not release them, and must not retain anything derived from
/// them past the call.
///
/// **Invariant:** when present, `literal` is a length-1 array whose type is
/// exactly the type described by `field`.
#[repr(C)]
pub struct ArgDescriptor {
    /// The argument's field (name + type + metadata). Always present.
    pub field: ArrowSchema,

    /// The argument's constant value as a length-1 array, or a released array
    /// (`release == NULL`) when the argument is not a foldable constant.
    pub literal: ArrowArray,
}

impl ArgDescriptor {
    /// Build a descriptor from a field and an optional literal value.
    pub fn new(field: ArrowSchema, literal: Option<ArrowArray>) -> Self {
        Self {
            field,
            literal: literal.unwrap_or_else(ArrowArray::empty),
        }
    }

    /// The argument's field.
    pub fn field(&self) -> &ArrowSchema {
        &self.field
    }

    /// The argument's constant value, or `None` if it is not a foldable constant.
    pub fn literal(&self) -> Option<&ArrowArray> {
        (!self.literal.is_released()).then_some(&self.literal)
    }

    /// Whether the argument folds to a constant at planning time.
    pub fn has_literal(&self) -> bool {
        !self.literal.is_released()
    }

    /// Release both members, leaving the descriptor in the released state.
    ///
    /// # Safety
    ///
    /// The caller must own this descriptor; see [`ArrowSchema::release`].
    pub unsafe fn release(&mut self) {
        unsafe {
            self.field.release();
            self.literal.release();
        }
    }
}

// SAFETY: ArgDescriptor is two plain C structs, both of which are Send.
unsafe impl Send for ArgDescriptor {}

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

    /// Compute the output field given the input argument descriptors.
    ///
    /// `args` points to `args_count` [`ArgDescriptor`]s, each carrying the
    /// argument's field plus its value when it folds to a constant. The host
    /// owns the descriptors and releases them once this call returns.
    /// On success, writes the result schema to `*ret`.
    /// On error, writes a null-terminated message to `*errmsg`
    /// (freed by `FFI_Module::free_string`).
    ///
    /// Returns 0 on success, non-zero on error.
    pub get_return_field: unsafe extern "C" fn(
        ctx: *const c_void,
        args: *const ArgDescriptor,
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

/// Virtual function table for an aggregate function (UDAF).
///
/// Follows a three-stage pipeline: Aggregation → Combination →
/// Finalization. Intermediate state is
/// exchanged as a single Struct array at the FFI boundary — the children
/// of the Struct are the individual state fields.
#[repr(C)]
pub struct FFI_AggregateFunction {
    /// Opaque module-side context pointer.
    pub ctx: *const c_void,

    /// Return the function name as a null-terminated UTF-8 string.
    pub name: unsafe extern "C" fn(ctx: *const c_void) -> *const c_char,

    /// Compute the output field schema given input field schemas.
    pub get_return_field: unsafe extern "C" fn(
        ctx: *const c_void,
        args: *const ArrowSchema,
        args_count: usize,
        ret_schema: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// Return the intermediate state schema as a Struct schema.
    ///
    /// The returned `ArrowSchema` has format `"+s"` (Struct) whose children
    /// are the individual state field schemas.
    pub get_state_schema: unsafe extern "C" fn(
        ctx: *const c_void,
        args: *const ArrowSchema,
        args_count: usize,
        ret_schema: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// **Aggregation.** Process input arrays and produce a single-row
    /// Struct array of partial state.
    pub aggregate: unsafe extern "C" fn(
        ctx: *const c_void,
        args: *const ArrowArray,
        args_schemas: *const ArrowSchema,
        args_count: usize,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// **Combination.** Merge a multi-row Struct array of partial states
    /// into a single-row Struct array.
    pub combine: unsafe extern "C" fn(
        ctx: *const c_void,
        state_array: *const ArrowArray,
        state_schema: *const ArrowSchema,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// **Finalization.** Produce the final output value from a single-row
    /// Struct state array.
    pub finalize: unsafe extern "C" fn(
        ctx: *const c_void,
        state_array: *const ArrowArray,
        state_schema: *const ArrowSchema,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int,

    /// Finalize the function, freeing all owned resources.
    pub fini: unsafe extern "C" fn(ctx: *mut c_void),
}

// SAFETY: The vtable is function pointers plus an opaque ctx pointer.
unsafe impl Send for FFI_AggregateFunction {}
unsafe impl Sync for FFI_AggregateFunction {}

/// Host-side session context passed to a module's `init` function.
///
/// The module calls `define_function` / `define_aggregate_function` to register extensions.
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

    /// Register an aggregate function with the host session.
    ///
    /// The host takes ownership of `function` on success.
    /// Returns 0 on success, non-zero on error.
    pub define_aggregate_function:
        unsafe extern "C" fn(ctx: *mut c_void, function: FFI_AggregateFunction) -> c_int,
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

        // FFI_AggregateFunction: ctx + name + get_return_field + get_state_schema
        //   + aggregate + combine + finalize + fini = 8 pointers
        assert_eq!(std::mem::size_of::<FFI_AggregateFunction>(), 8 * ptr);

        // FFI_SessionContext: ctx + define_function + define_aggregate_function = 3 pointers
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
        assert_send_sync::<FFI_AggregateFunction>();
        assert_send_sync::<FFI_SessionContext>();
        assert_send_sync::<FFI_Module>();
    }

    #[test]
    fn constants() {
        // !! THIS TEST EXISTS SO THAT THESE ARE NOT CHANGED BY ACCIDENT
        // IT MEANS WE HAVE TO MANUALLY UPDATE IN TWO PLACES !!
        assert_eq!(DAFT_ABI_VERSION, 2);
        assert_eq!(DAFT_MODULE_MAGIC_SYMBOL, "daft_module_magic");
    }

    #[test]
    fn arg_descriptor_layout() {
        // The descriptor is two C Data Interface structs back to back; C/C++
        // modules mirror this layout by hand.
        assert_eq!(
            std::mem::size_of::<ArgDescriptor>(),
            std::mem::size_of::<ArrowSchema>() + std::mem::size_of::<ArrowArray>()
        );
    }

    #[test]
    fn arg_descriptor_literal_presence() {
        let without = ArgDescriptor::new(ArrowSchema::empty(), None);
        assert!(!without.has_literal());
        assert!(without.literal().is_none());

        // A released array is indistinguishable from an absent literal.
        let released = ArgDescriptor::new(ArrowSchema::empty(), Some(ArrowArray::empty()));
        assert!(!released.has_literal());
    }
}
