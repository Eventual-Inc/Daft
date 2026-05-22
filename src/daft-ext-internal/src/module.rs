//! Extension module loading (`dlopen`) and global cache.

use std::{
    collections::HashMap,
    ffi::{CStr, c_char},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use common_error::{DaftError, DaftResult};
use daft_ext::abi::{DAFT_ABI_VERSION, DAFT_MODULE_MAGIC_SYMBOL, FFI_Module};
use libloading::Library;

use crate::{aggregate::AggregateFunctionHandle, function::ScalarFunctionHandle};

/// A shared handle to a loaded extension module.
///
/// Wraps an `FFI_Module` and provides safe accessors for module-level
/// callbacks (e.g. `free_string`). Functions from the same module share
/// one `Arc<ModuleHandle>`.
pub struct ModuleHandle {
    module: FFI_Module,
    path: PathBuf,
}

impl ModuleHandle {
    /// Wraps a raw `FFI_Module` in a `ModuleHandle`.
    pub fn new(module: FFI_Module, path: PathBuf) -> Self {
        Self { module, path }
    }

    /// Path to the shared library this module was loaded from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Free a C string allocated by this module.
    ///
    /// # Safety
    /// `s` must be a valid pointer allocated by this module, or null.
    pub unsafe fn free_string(&self, s: *mut c_char) {
        if !s.is_null() {
            unsafe { (self.module.free_string)(s) };
        }
    }

    /// Module name (for diagnostics).
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.module.name) }
    }

    /// The raw FFI_Module (for calling `init`).
    pub fn ffi_module(&self) -> &FFI_Module {
        &self.module
    }
}

// SAFETY: FFI_Module is already Send + Sync (contains only fn ptrs and *const).
unsafe impl Send for ModuleHandle {}
unsafe impl Sync for ModuleHandle {}

/// A loaded extension module and all functions it registered during `init`.
///
/// `_library` is `Option<Library>` so that test helpers can insert stub entries
/// without a real shared library — production code always uses `Some`.
struct LoadedModule {
    /// The [`Library`] handle, kept alive for the lifetime of the process.
    _library: Option<Library>,
    /// Shared handle to the module's FFI descriptor.
    handle: Arc<ModuleHandle>,
    /// Scalar functions registered by this module during `init`.
    scalar_fns: HashMap<String, ScalarFunctionHandle>,
    /// Aggregate functions registered by this module during `init`.
    agg_fns: HashMap<String, AggregateFunctionHandle>,
}

/// A global, thread-safe store of loaded extension modules and their functions.
///
/// This is the single source of truth for all extension state in a process.
/// Scalar and aggregate function handles live here (within each module's entry)
/// rather than in separate top-level registries.
static MODULES: OnceLock<Mutex<HashMap<PathBuf, LoadedModule>>> = OnceLock::new();

/// Loads an extension module from a shared library, returning a shared [`ModuleHandle`].
pub fn load_module(path: &Path) -> DaftResult<Arc<ModuleHandle>> {
    let path = path.canonicalize().map_err(|e| {
        DaftError::InternalError(format!("failed to canonicalize '{}': {e}", path.display()))
    })?;

    let mut modules = MODULES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|e| DaftError::InternalError(format!("extension store lock poisoned: {e}")))?;

    if let Some(entry) = modules.get(&path) {
        return Ok(entry.handle.clone());
    }

    let library = open_library(&path)?;
    let module = resolve_module(&library, &path)?;
    let handle = Arc::new(ModuleHandle::new(module, path.clone()));
    modules.insert(
        path,
        LoadedModule {
            _library: Some(library),
            handle: handle.clone(),
            scalar_fns: HashMap::new(),
            agg_fns: HashMap::new(),
        },
    );

    Ok(handle)
}

/// Register a scalar function handle in the module's `MODULES` entry.
///
/// Called by [`crate::function::into_scalar_function_factory`] during extension
/// initialization. Idempotent — re-registering the same `(path, name)` overwrites.
pub fn register_extension_function(path: &Path, name: String, handle: ScalarFunctionHandle) {
    match MODULES.get_or_init(|| Mutex::new(HashMap::new())).lock() {
        Ok(mut guard) => {
            if let Some(entry) = guard.get_mut(path) {
                entry.scalar_fns.insert(name, handle);
            } else {
                log::warn!(
                    "register_extension_function: module '{}' not in registry",
                    path.display()
                );
            }
        }
        Err(e) => log::warn!("MODULES lock poisoned in register_extension_function: {e}"),
    }
}

/// Look up a scalar function handle by module path and function name.
///
/// Returns `None` if the module is not loaded in this process or the function
/// was never registered.
pub fn lookup_extension_function(path: &Path, name: &str) -> Option<ScalarFunctionHandle> {
    let guard = MODULES
        .get()?
        .lock()
        .map_err(|e| log::warn!("MODULES lock poisoned in lookup_extension_function: {e}"))
        .ok()?;
    guard.get(path)?.scalar_fns.get(name).cloned()
}

/// Register an aggregate function handle in the module's `MODULES` entry.
///
/// Called by [`crate::aggregate::into_aggregate_fn_handle`] during extension
/// initialization. Idempotent — re-registering the same `(path, name)` overwrites.
pub fn register_extension_aggregate(path: &Path, name: String, handle: AggregateFunctionHandle) {
    match MODULES.get_or_init(|| Mutex::new(HashMap::new())).lock() {
        Ok(mut guard) => {
            if let Some(entry) = guard.get_mut(path) {
                entry.agg_fns.insert(name, handle);
            } else {
                log::warn!(
                    "register_extension_aggregate: module '{}' not in registry",
                    path.display()
                );
            }
        }
        Err(e) => log::warn!("MODULES lock poisoned in register_extension_aggregate: {e}"),
    }
}

/// Look up an aggregate function handle by module path and function name.
///
/// Returns `None` if the module is not loaded in this process or the function
/// was never registered.
pub fn lookup_extension_aggregate(path: &Path, name: &str) -> Option<AggregateFunctionHandle> {
    let guard = MODULES
        .get()?
        .lock()
        .map_err(|e| log::warn!("MODULES lock poisoned in lookup_extension_aggregate: {e}"))
        .ok()?;
    guard.get(path)?.agg_fns.get(name).cloned()
}

/// Return all paths currently in the process-global `MODULES` cache.
///
/// Order is unspecified. Intended for propagating the driver's loaded
/// extension set to Ray workers.
pub fn loaded_module_paths() -> Vec<PathBuf> {
    let Some(modules) = MODULES.get() else {
        return Vec::new();
    };
    let Ok(guard) = modules.lock() else {
        return Vec::new();
    };
    guard.keys().cloned().collect()
}

/// Opens a shared library from disk, returning a [`Library`] handle.
fn open_library(path: &Path) -> DaftResult<Library> {
    // SAFETY: We trust that the shared library at `path` is a well-formed
    // Daft extension module that exports the expected symbol.
    unsafe { Library::new(path) }
        .map_err(|e| DaftError::InternalError(format!("failed to load '{}': {e}", path.display())))
}

/// Resolves and validates the [`FFI_Module`] descriptor from a loaded library.
fn resolve_module(library: &Library, path: &Path) -> DaftResult<FFI_Module> {
    // SAFETY: The symbol must be an `extern "C" fn() -> FFI_Module`.
    let entry_fn = unsafe {
        library.get::<unsafe extern "C" fn() -> FFI_Module>(DAFT_MODULE_MAGIC_SYMBOL.as_bytes())
    }
    .map_err(|e| {
        DaftError::InternalError(format!(
            "symbol '{DAFT_MODULE_MAGIC_SYMBOL}' not found in '{}': {e}",
            path.display()
        ))
    })?;

    // SAFETY: Calling the entry point to obtain the module descriptor.
    let module = unsafe { entry_fn() };

    if module.daft_abi_version != DAFT_ABI_VERSION {
        let name = unsafe { std::ffi::CStr::from_ptr(module.name) }
            .to_str()
            .unwrap_or("<invalid utf8>");
        return Err(DaftError::InternalError(format!(
            "extension '{name}' has ABI version {}, expected {DAFT_ABI_VERSION}",
            module.daft_abi_version
        )));
    }

    Ok(module)
}

/// Insert a stub module entry into `MODULES` for use in tests.
///
/// Uses `or_insert` so it is a no-op if the path is already registered.
/// The stub has no real library (`_library: None`) and empty function maps.
/// Call [`register_extension_function`] / [`register_extension_aggregate`] afterwards
/// to populate the function maps for the test.
#[cfg(test)]
pub(crate) fn insert_test_module(path: PathBuf) {
    use std::ffi::c_int;

    use daft_ext::abi::{FFI_Module, FFI_SessionContext};

    unsafe extern "C" fn noop_init(_: *mut FFI_SessionContext) -> c_int {
        0
    }
    unsafe extern "C" fn noop_free(_: *mut c_char) {}

    let ffi_module = FFI_Module {
        daft_abi_version: DAFT_ABI_VERSION,
        name: c"test_stub".as_ptr(),
        init: noop_init,
        free_string: noop_free,
    };
    let handle = Arc::new(ModuleHandle::new(ffi_module, path.clone()));
    let mut guard = MODULES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    guard.entry(path).or_insert(LoadedModule {
        _library: None,
        handle,
        scalar_fns: HashMap::new(),
        agg_fns: HashMap::new(),
    });
}

#[cfg(test)]
mod tests {
    use std::ffi::c_int;

    use daft_ext::abi::{DAFT_ABI_VERSION, FFI_Module, FFI_SessionContext};

    use super::*;

    unsafe extern "C" fn noop_init(_: *mut FFI_SessionContext) -> c_int {
        0
    }

    unsafe extern "C" fn noop_free(_: *mut c_char) {}

    fn make_test_module(name: &'static CStr) -> FFI_Module {
        FFI_Module {
            daft_abi_version: DAFT_ABI_VERSION,
            name: name.as_ptr(),
            init: noop_init,
            free_string: noop_free,
        }
    }

    #[test]
    fn module_handle_name() {
        let module = make_test_module(c"test_module");
        let handle = ModuleHandle::new(module, Path::new("/mock/test_module").to_path_buf());
        assert_eq!(handle.name().to_str().unwrap(), "test_module");
    }

    #[test]
    fn module_handle_ffi_module() {
        let module = make_test_module(c"test_module");
        let handle = ModuleHandle::new(module, Path::new("/mock/test_module").to_path_buf());
        assert_eq!(handle.ffi_module().daft_abi_version, DAFT_ABI_VERSION);
    }

    #[test]
    fn module_handle_free_string_null() {
        let module = make_test_module(c"test_module");
        let handle = ModuleHandle::new(module, Path::new("/mock/test_module").to_path_buf());
        // Should not panic on null
        unsafe { handle.free_string(std::ptr::null_mut()) };
    }

    #[test]
    fn module_handle_exposes_path() {
        let module = make_test_module(c"test_module");
        let path = Path::new("/tmp/fake.so").to_path_buf();
        let handle = ModuleHandle::new(module, path.clone());
        assert_eq!(handle.path(), path.as_path());
    }

    #[test]
    fn load_module_nonexistent_path() {
        let result = load_module(Path::new("/nonexistent/path/libfake.so"));
        match result {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("failed to canonicalize"),
                    "unexpected error: {msg}"
                );
            }
            Ok(_) => panic!("expected error for nonexistent path"),
        }
    }

    #[test]
    fn lookup_unknown_module_returns_none() {
        assert!(lookup_extension_function(Path::new("/no/such/module.so"), "some_fn").is_none());
        assert!(lookup_extension_aggregate(Path::new("/no/such/module.so"), "some_agg").is_none());
    }
}
