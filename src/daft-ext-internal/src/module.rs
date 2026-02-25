//! Extension module loading (`dlopen`) and global cache.

use std::{
    collections::HashMap,
    ffi::{CStr, c_char},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use common_error::{DaftError, DaftResult};
use daft_ext_abi::{DAFT_ABI_VERSION, DAFT_MODULE_MAGIC_SYMBOL, FFI_Module};
use libloading::Library;

/// A shared handle to a loaded extension module.
///
/// Wraps an `FFI_Module` and provides safe accessors for module-level
/// callbacks (e.g. `free_string`). Functions from the same module share
/// one `Arc<ModuleHandle>`.
pub struct ModuleHandle {
    module: FFI_Module,
}

impl ModuleHandle {
    /// Wraps a raw `FFI_Module` in a `ModuleHandle`.
    pub fn new(module: FFI_Module) -> Self {
        Self { module }
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

/// A global, thread-safe store of loaded extension modules.
static MODULES: OnceLock<Mutex<HashMap<PathBuf, LoadedModule>>> = OnceLock::new();

/// A loaded extension module.
struct LoadedModule {
    /// The [`Library`] handle, kept alive for the lifetime of the process.
    _library: Library,
    /// Shared handle to the module's FFI descriptor.
    handle: Arc<ModuleHandle>,
}

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
    let handle = Arc::new(ModuleHandle::new(module));
    modules.insert(
        path,
        LoadedModule {
            _library: library,
            handle: handle.clone(),
        },
    );

    Ok(handle)
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
