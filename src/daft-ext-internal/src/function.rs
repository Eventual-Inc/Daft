use std::{
    collections::HashMap,
    ffi::{CStr, c_char},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{
        BuiltinScalarFnVariant, FunctionArgs, ScalarFunctionFactory, ScalarUDF, scalar::EvalContext,
    },
};
use daft_ext::abi::{ArrowArray, ArrowSchema, FFI_ScalarFunction};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::module::ModuleHandle;

/// Bundles an `FFI_ScalarFunction` vtable with its parent `ModuleHandle`.
///
/// The `ModuleHandle` is kept alive so that `free_string` remains valid
/// for the lifetime of the vtable.
struct Inner {
    ffi: FFI_ScalarFunction,
    module: Arc<ModuleHandle>,
}

impl Inner {
    /// Read and free an FFI-allocated C string, returning the owned Rust string.
    unsafe fn take_ffi_string(&self, ptr: *mut c_char) -> String {
        let s = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { self.module.free_string(ptr) };
        s
    }

    /// Check an FFI return code, converting a non-zero result into a `DaftError`.
    fn check(&self, rc: i32, errmsg: *mut c_char, default_msg: &str) -> DaftResult<()> {
        if rc == 0 {
            return Ok(());
        }
        let msg = if errmsg.is_null() {
            default_msg.to_string()
        } else {
            unsafe { self.take_ffi_string(errmsg) }
        };
        Err(DaftError::InternalError(msg))
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        unsafe { (self.ffi.fini)(self.ffi.ctx.cast_mut()) };
    }
}

// SAFETY: The FFI_ScalarFunction is already Send + Sync; ModuleHandle is Send + Sync.
unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

/// Process-global registry of loaded extension functions.
///
/// Populated during `Session::load_and_init_extension`, consulted by
/// `ScalarFunctionHandle::deserialize` to re-attach the FFI vtable on
/// processes where the plan was deserialized (e.g. Ray workers).
///
/// Keyed by `(module_path, function_name)`. Handles are stored by value;
/// clones are cheap (the inner state is `Arc`-shared).
static FUNCTION_REGISTRY: OnceLock<Mutex<HashMap<(PathBuf, String), ScalarFunctionHandle>>> =
    OnceLock::new();

/// Register a function handle in the process-global registry. Idempotent —
/// re-registering the same `(path, name)` overwrites the previous entry.
pub fn register_function(path: PathBuf, name: String, handle: ScalarFunctionHandle) {
    let mut reg = FUNCTION_REGISTRY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("FUNCTION_REGISTRY lock poisoned");
    reg.insert((path, name), handle);
}

/// Look up a registered function by module path and function name.
pub fn lookup_function(path: &Path, name: &str) -> Option<ScalarFunctionHandle> {
    let reg = FUNCTION_REGISTRY.get()?.lock().ok()?;
    reg.get(&(path.to_path_buf(), name.to_string())).cloned()
}

/// Wraps an `FFI_ScalarFunction` vtable (from a loaded extension module)
/// into Daft's `ScalarUDF` and `ScalarFunctionFactory` traits.
#[derive(Clone)]
pub struct ScalarFunctionHandle {
    name: &'static str,
    module_path: PathBuf,
    inner: Option<Arc<Inner>>,
}

impl ScalarFunctionHandle {
    /// Create from an `FFI_ScalarFunction` vtable and its parent `ModuleHandle`.
    fn new(ffi: FFI_ScalarFunction, module: Arc<ModuleHandle>) -> Self {
        let name_ptr = unsafe { (ffi.name)(ffi.ctx) };
        let name: &'static str = Box::leak(
            unsafe { CStr::from_ptr(name_ptr) }
                .to_str()
                .expect("FFI function name must be valid UTF-8")
                .to_owned()
                .into_boxed_str(),
        );
        let module_path = module.path().to_path_buf();
        Self {
            name,
            module_path,
            inner: Some(Arc::new(Inner { ffi, module })),
        }
    }

    /// Returns the inner FFI handle, or an error if deserialized without a loaded extension.
    fn inner(&self) -> DaftResult<&Inner> {
        self.inner.as_deref().ok_or_else(|| {
            DaftError::InternalError(format!("extension function '{}' is not loaded", self.name,))
        })
    }
}

#[typetag::serde]
impl ScalarUDF for ScalarFunctionHandle {
    fn name(&self) -> &'static str {
        self.name
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let inner = self.inner()?;

        let arrow_fields: Vec<arrow_schema::Field> = args
            .into_inner()
            .into_iter()
            .map(|expr| expr.to_field(schema)?.to_arrow())
            .collect::<DaftResult<_>>()?;

        let ffi_schemas: Vec<ArrowSchema> = arrow_fields
            .iter()
            .map(|f| {
                let ffi = arrow::ffi::FFI_ArrowSchema::try_from(f)
                    .map_err(|e| DaftError::InternalError(format!("schema export failed: {e}")))?;
                Ok(unsafe { ArrowSchema::from_owned(ffi) })
            })
            .collect::<DaftResult<_>>()?;

        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (inner.ffi.get_return_field)(
                inner.ffi.ctx,
                ffi_schemas.as_ptr(),
                ffi_schemas.len(),
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "unknown error in extension get_return_field")?;

        let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { ret_schema.into_owned() };
        let arrow_field = arrow_schema::Field::try_from(&ffi_schema)
            .map_err(|e| DaftError::InternalError(format!("schema import failed: {e}")))?;

        Field::try_from(&arrow_field)
    }

    fn call(&self, args: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inner = self.inner()?;
        let series_vec: Vec<Series> = args.into_inner();

        let mut ffi_arrays: Vec<ArrowArray> = Vec::with_capacity(series_vec.len());
        let mut ffi_schemas: Vec<ArrowSchema> = Vec::with_capacity(series_vec.len());

        for s in &series_vec {
            let array = s.to_arrow()?;
            let target_field = s.field().to_arrow()?;

            let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(target_field)?;
            let mut data = array.to_data();
            data.align_buffers();
            let ffi_array = arrow::ffi::FFI_ArrowArray::new(&data);
            ffi_arrays.push(unsafe { ArrowArray::from_owned(ffi_array) });
            ffi_schemas.push(unsafe { ArrowSchema::from_owned(ffi_schema) });
        }

        // Get expected return field via get_return_field (the call FFI only
        // returns a bare array whose schema has an empty field name).
        let mut ret_field_schema = ArrowSchema::empty();
        let mut field_errmsg: *mut c_char = std::ptr::null_mut();
        let field_rc = unsafe {
            (inner.ffi.get_return_field)(
                inner.ffi.ctx,
                ffi_schemas.as_ptr(),
                ffi_schemas.len(),
                &raw mut ret_field_schema,
                &raw mut field_errmsg,
            )
        };
        inner.check(
            field_rc,
            field_errmsg,
            "error in extension get_return_field",
        )?;

        let ffi_field_schema: arrow::ffi::FFI_ArrowSchema =
            unsafe { ret_field_schema.into_owned() };
        let ret_arrow_field = arrow_schema::Field::try_from(&ffi_field_schema)
            .map_err(|e| DaftError::InternalError(format!("schema import failed: {e}")))?;
        let ret_daft_field = Field::try_from(&ret_arrow_field)?;

        // Execute the function.
        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (inner.ffi.call)(
                inner.ffi.ctx,
                ffi_arrays.as_ptr(),
                ffi_schemas.as_ptr(),
                ffi_arrays.len(),
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "unknown error in extension call")?;

        let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { ret_array.into_owned() };
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { ret_schema.into_owned() };
        let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }
            .map_err(|e| DaftError::InternalError(format!("Arrow FFI import failed: {e}")))?;
        let result_arr = arrow_array::make_array(arrow_data);

        Series::from_arrow(ret_daft_field, result_arr)
    }

    fn docstring(&self) -> &'static str {
        "External extension function"
    }
}

impl Serialize for ScalarFunctionHandle {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("ScalarFunctionHandle", 2)?;
        s.serialize_field("name", self.name)?;
        s.serialize_field("module_path", &self.module_path)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for ScalarFunctionHandle {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            name: String,
            // Optional for forward-compat with older serialized plans that
            // predate this field.
            #[serde(default)]
            module_path: Option<PathBuf>,
        }
        let h = Helper::deserialize(deserializer)?;

        // If the module is loaded in this process, return the live handle
        // directly so `inner` is populated and FFI calls work.
        if let Some(path) = &h.module_path
            && let Some(existing) = lookup_function(path, &h.name)
        {
            return Ok(existing);
        }

        // Fall back to a handle with `inner: None`. Calls will error with
        // `extension function 'X' is not loaded`, which is the correct
        // behavior when the worker hasn't loaded the extension.
        let name: &'static str = Box::leak(h.name.into_boxed_str());
        Ok(Self {
            name,
            module_path: h.module_path.unwrap_or_default(),
            inner: None,
        })
    }
}

impl ScalarFunctionFactory for ScalarFunctionHandle {
    fn name(&self) -> &'static str {
        self.name
    }

    fn get_function(
        &self,
        _args: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<BuiltinScalarFnVariant> {
        Ok(BuiltinScalarFnVariant::Sync(Arc::new(self.clone())))
    }
}

/// Create a [`ScalarFunctionFactory`] from an `FFI_ScalarFunction` vtable.
///
/// Called by the extension loader when a module invokes `define_function`
/// during initialization. As a side effect, this also registers the function
/// in the process-global `FUNCTION_REGISTRY` so that deserialized handles in
/// other parts of the process (or on Ray workers after they load the same
/// `.so`) can re-attach their FFI vtable.
pub fn into_scalar_function_factory(
    ffi: FFI_ScalarFunction,
    module: Arc<ModuleHandle>,
) -> Arc<dyn ScalarFunctionFactory> {
    let path = module.path().to_path_buf();
    let handle = ScalarFunctionHandle::new(ffi, module);
    let name = handle.name.to_string();
    register_function(path, name, handle.clone());
    Arc::new(handle)
}

#[cfg(test)]
mod tests {
    use std::ffi::{CString, c_int, c_void};

    use arrow_array::Int32Array;

    use super::*;

    struct IncrementCtx;

    unsafe extern "C" fn mock_name(_ctx: *const c_void) -> *const c_char {
        c"increment".as_ptr()
    }

    unsafe extern "C" fn mock_get_return_field(
        _ctx: *const c_void,
        _args: *const ArrowSchema,
        _args_count: usize,
        ret: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        let field = arrow_schema::Field::new("result", arrow_schema::DataType::Int32, false);
        let schema: ArrowSchema = unsafe {
            ArrowSchema::from_owned(arrow::ffi::FFI_ArrowSchema::try_from(&field).unwrap())
        };
        unsafe { std::ptr::write(ret, schema) };
        0
    }

    unsafe extern "C" fn mock_call(
        _ctx: *const c_void,
        args: *const ArrowArray,
        args_schemas: *const ArrowSchema,
        args_count: usize,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        assert_eq!(args_count, 1);
        let abi_array = unsafe { std::ptr::read(args) };
        let abi_schema = unsafe { std::ptr::read(args_schemas) };
        let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { abi_array.into_owned() };
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { abi_schema.into_owned() };
        let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }.unwrap();
        let arr = arrow_array::make_array(data);
        let input = arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array")
            .clone();

        let output: Int32Array = input.iter().map(|v| v.map(|x| x + 1)).collect();
        let output_ref: arrow_array::ArrayRef = Arc::new(output);
        let (ffi_arr, ffi_sch) = arrow::ffi::to_ffi(&output_ref.to_data()).unwrap();
        let out_array: ArrowArray = unsafe { ArrowArray::from_owned(ffi_arr) };
        let out_schema: ArrowSchema = unsafe { ArrowSchema::from_owned(ffi_sch) };
        unsafe {
            std::ptr::write(ret_array, out_array);
            std::ptr::write(ret_schema, out_schema);
        }
        0
    }

    unsafe extern "C" fn mock_fini(ctx: *mut c_void) {
        if !ctx.is_null() {
            unsafe { drop(Box::from_raw(ctx.cast::<IncrementCtx>())) };
        }
    }

    unsafe extern "C" fn mock_free_string(s: *mut c_char) {
        if !s.is_null() {
            unsafe { drop(CString::from_raw(s)) };
        }
    }

    unsafe extern "C" fn mock_init(_session: *mut daft_ext::abi::FFI_SessionContext) -> c_int {
        0
    }

    fn make_mock_module_handle() -> Arc<ModuleHandle> {
        use daft_ext::abi::FFI_Module;
        let module = FFI_Module {
            daft_abi_version: daft_ext::abi::DAFT_ABI_VERSION,
            name: c"mock_module".as_ptr(),
            init: mock_init,
            free_string: mock_free_string,
        };
        Arc::new(ModuleHandle::new(
            module,
            std::path::PathBuf::from("/mock/mock_module"),
        ))
    }

    fn make_mock_handle() -> (FFI_ScalarFunction, Arc<ModuleHandle>) {
        let ctx = Box::into_raw(Box::new(IncrementCtx));
        let handle = FFI_ScalarFunction {
            ctx: ctx.cast(),
            name: mock_name,
            get_return_field: mock_get_return_field,
            call: mock_call,
            fini: mock_fini,
        };
        (handle, make_mock_module_handle())
    }

    #[test]
    fn test_name() {
        let (ffi, module) = make_mock_handle();
        let udf = ScalarFunctionHandle::new(ffi, module);
        assert_eq!(ScalarUDF::name(&udf), "increment");
    }

    #[test]
    fn test_get_return_field() {
        let (ffi, module) = make_mock_handle();
        let udf = ScalarFunctionHandle::new(ffi, module);

        let schema = Schema::new(vec![Field::new("x", DataType::Int32)]);
        let args = FunctionArgs::new_unnamed(vec![daft_dsl::resolved_col("x")]);

        let field = udf.get_return_field(args, &schema).unwrap();
        assert_eq!(field.name.as_ref(), "result");
        assert_eq!(field.dtype, DataType::Int32);
    }

    #[test]
    fn test_call() {
        let (ffi, module) = make_mock_handle();
        let udf = ScalarFunctionHandle::new(ffi, module);

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let series = Series::from_arrow(Field::new("x", DataType::Int32), arrow_arr).unwrap();

        let args = FunctionArgs::new_unnamed(vec![series]);
        let ctx = EvalContext { row_count: 3 };
        let result = udf.call(args, &ctx).unwrap();

        let result_int = result
            .to_arrow()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!(result_int.values().as_ref(), &[2, 3, 4]);
    }

    #[test]
    fn test_serde_roundtrip_no_module_loaded() {
        // Use a path that is never registered in any other test so the
        // registry lookup always misses and `inner` stays `None`.
        use daft_ext::abi::FFI_Module;
        let unregistered_module = Arc::new(ModuleHandle::new(
            FFI_Module {
                daft_abi_version: daft_ext::abi::DAFT_ABI_VERSION,
                name: c"mock_module".as_ptr(),
                init: mock_init,
                free_string: mock_free_string,
            },
            std::path::PathBuf::from("/mock/test_serde_roundtrip_no_module_loaded/mock_module"),
        ));
        let (ffi, _module) = make_mock_handle();
        let udf = ScalarFunctionHandle::new(ffi, unregistered_module);

        let json = serde_json::to_string(&udf).unwrap();
        assert!(json.contains("increment"));

        let deser: ScalarFunctionHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(ScalarUDF::name(&deser), "increment");
        assert!(deser.inner.is_none());

        // Calling on a deserialized (unloaded) UDF should error.
        let schema = Schema::new(vec![Field::new("x", DataType::Int32)]);
        let args = FunctionArgs::new_unnamed(vec![daft_dsl::resolved_col("x")]);
        let err = deser.get_return_field(args, &schema).unwrap_err();
        assert!(
            err.to_string().contains("not loaded"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_serde_roundtrip_with_module_loaded() {
        // Simulate a worker environment: the plan has been serialized by the
        // driver and arrives here to be deserialized. The worker process has
        // already loaded the extension (e.g. via the Ray actor init hook),
        // which in a real flow populates FUNCTION_REGISTRY. We populate it
        // directly here with a unique path so the test doesn't interact with
        // other tests that might register "increment".
        let (ffi, module) = make_mock_handle();
        let path =
            std::path::PathBuf::from("/mock/test_serde_roundtrip_with_module_loaded/mock_module");
        // Override the module's path for this test by constructing a fresh
        // ModuleHandle with the same FFI_Module but our unique path.
        let module = Arc::new(ModuleHandle::new(*module.ffi_module(), path.clone()));
        let live_handle = ScalarFunctionHandle::new(ffi, module);
        register_function(path.clone(), "increment".to_string(), live_handle.clone());

        // Driver: serialize.
        let json = serde_json::to_string(&live_handle).unwrap();
        assert!(json.contains("increment"));
        assert!(json.contains(path.to_str().unwrap()));

        // Worker: deserialize. Since FUNCTION_REGISTRY has a match,
        // `inner` should be populated.
        let deser: ScalarFunctionHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(ScalarUDF::name(&deser), "increment");
        assert!(
            deser.inner.is_some(),
            "expected inner to be re-attached from FUNCTION_REGISTRY"
        );

        // And calls should actually work.
        let arrow_arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let series = Series::from_arrow(Field::new("x", DataType::Int32), arrow_arr).unwrap();
        let args = FunctionArgs::new_unnamed(vec![series]);
        let ctx = EvalContext { row_count: 3 };
        let result = deser.call(args, &ctx).unwrap();

        let result_int = result
            .to_arrow()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!(result_int.values().as_ref(), &[11, 21, 31]);
    }

    #[test]
    fn test_factory() {
        let (ffi, module) = make_mock_handle();
        let factory = into_scalar_function_factory(ffi, module);
        assert_eq!(factory.name(), "increment");
    }

    // --- mock that asserts incoming schema carries extension metadata ---

    use std::sync::atomic::{AtomicBool, Ordering};

    static METADATA_SEEN: AtomicBool = AtomicBool::new(false);

    unsafe extern "C" fn meta_mock_name(_ctx: *const c_void) -> *const c_char {
        c"meta_passthrough".as_ptr()
    }

    unsafe extern "C" fn meta_mock_get_return_field(
        _ctx: *const c_void,
        _args: *const ArrowSchema,
        _args_count: usize,
        ret: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        let field = arrow_schema::Field::new("result", arrow_schema::DataType::Int32, false);
        let schema: ArrowSchema = unsafe {
            ArrowSchema::from_owned(arrow::ffi::FFI_ArrowSchema::try_from(&field).unwrap())
        };
        unsafe { std::ptr::write(ret, schema) };
        0
    }

    unsafe extern "C" fn meta_mock_call(
        _ctx: *const c_void,
        args: *const ArrowArray,
        args_schemas: *const ArrowSchema,
        args_count: usize,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        assert_eq!(args_count, 1);

        let abi_schema = unsafe { std::ptr::read(args_schemas) };
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { abi_schema.into_owned() };
        let field = arrow_schema::Field::try_from(&ffi_schema).unwrap();

        let has_ext_name = field
            .metadata()
            .get("ARROW:extension:name")
            .is_some_and(|v| v == "my_ext_type");
        let has_ext_meta = field
            .metadata()
            .get("ARROW:extension:metadata")
            .is_some_and(|v| v == "ext_meta_payload");

        METADATA_SEEN.store(has_ext_name && has_ext_meta, Ordering::SeqCst);

        let abi_array = unsafe { std::ptr::read(args) };
        let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { abi_array.into_owned() };
        let ffi_schema2: arrow::ffi::FFI_ArrowSchema = arrow::ffi::FFI_ArrowSchema::try_from(
            &arrow_schema::Field::new("x", arrow_schema::DataType::Int32, true),
        )
        .unwrap();
        let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema2) }.unwrap();
        let arr = arrow_array::make_array(data);
        let output_ref: arrow_array::ArrayRef = arr;
        let (ffi_arr, ffi_sch) = arrow::ffi::to_ffi(&output_ref.to_data()).unwrap();
        let out_array: ArrowArray = unsafe { ArrowArray::from_owned(ffi_arr) };
        let out_schema: ArrowSchema = unsafe { ArrowSchema::from_owned(ffi_sch) };
        unsafe {
            std::ptr::write(ret_array, out_array);
            std::ptr::write(ret_schema, out_schema);
        }
        0
    }

    fn make_meta_mock_handle() -> (FFI_ScalarFunction, Arc<ModuleHandle>) {
        let ctx = Box::into_raw(Box::new(IncrementCtx));
        let handle = FFI_ScalarFunction {
            ctx: ctx.cast(),
            name: meta_mock_name,
            get_return_field: meta_mock_get_return_field,
            call: meta_mock_call,
            fini: mock_fini,
        };
        (handle, make_mock_module_handle())
    }

    #[test]
    fn test_call_preserves_field_metadata() {
        METADATA_SEEN.store(false, Ordering::SeqCst);

        let (ffi, module) = make_meta_mock_handle();
        let udf = ScalarFunctionHandle::new(ffi, module);

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let field = Field::new(
            "x",
            DataType::Extension(
                "my_ext_type".into(),
                Box::new(DataType::Int32),
                Some("ext_meta_payload".into()),
            ),
        );
        let series = Series::from_arrow(field, arrow_arr).unwrap();

        let args = FunctionArgs::new_unnamed(vec![series]);
        let ctx = EvalContext { row_count: 3 };
        let _result = udf.call(args, &ctx).unwrap();

        assert!(
            METADATA_SEEN.load(Ordering::SeqCst),
            "extension metadata was not preserved in the FFI schema"
        );
    }

    #[test]
    fn function_registry_roundtrip() {
        let (ffi, module) = make_mock_handle();
        let path = module.path().to_path_buf();
        let handle = ScalarFunctionHandle::new(ffi, module);
        register_function(path.clone(), "increment".to_string(), handle);

        let found = lookup_function(&path, "increment").expect("should find registered function");
        assert_eq!(ScalarUDF::name(&found), "increment");
        assert!(found.inner.is_some());
    }
}
