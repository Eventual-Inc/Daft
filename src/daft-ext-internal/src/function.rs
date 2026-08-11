use std::{
    ffi::{CStr, c_char},
    path::PathBuf,
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{
        BuiltinScalarFnVariant, FunctionArgs, ScalarFunctionFactory, ScalarUDF, scalar::EvalContext,
    },
};
use daft_ext::abi::{ArgDescriptor, ArrowArray, ArrowSchema, FFI_ScalarFunction};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::module::ModuleHandle;

/// Export a daft [`Field`] as a C Data Interface schema.
fn export_field(field: &Field) -> DaftResult<ArrowSchema> {
    let arrow_field = field.to_arrow()?;
    let ffi = arrow::ffi::FFI_ArrowSchema::try_from(&arrow_field)
        .map_err(|e| DaftError::InternalError(format!("schema export failed: {e}")))?;
    Ok(unsafe { ArrowSchema::from_owned(ffi) })
}

/// Materialize a literal as a length-1 array of `dtype`, for the ABI.
///
/// Returns `None` when the literal has no Arrow representation (Python objects,
/// for instance) or cannot be represented as `dtype`. Extensions see that as
/// "this argument is not a foldable constant", which is always a safe answer —
/// planning must never fail because a value could not be described.
fn export_literal(literal: &Literal, dtype: &DataType) -> Option<ArrowArray> {
    let series = Series::from_literals(vec![literal.clone()]).ok()?;
    // Literal types are lossy (`FixedSizeList` round-trips as `List`, and so
    // on), so restore the argument's declared type: extensions read the literal
    // through the descriptor's field.
    let series = if series.data_type() == dtype {
        series
    } else {
        series.cast(dtype).ok()?
    };
    let array = series.to_arrow().ok()?;
    let mut data = array.to_data();
    data.align_buffers();
    let ffi = arrow::ffi::FFI_ArrowArray::new(&data);
    Some(unsafe { ArrowArray::from_owned(ffi) })
}

/// Release every descriptor in `descriptors`.
///
/// The host owns the descriptors it passes to `get_return_field`; extensions
/// only borrow them. Neither [`ArrowSchema`] nor [`ArrowArray`] has a `Drop`
/// impl, so dropping the `Vec` without this would leak.
fn release_descriptors(descriptors: &mut [ArgDescriptor]) {
    for descriptor in descriptors {
        unsafe { descriptor.release() };
    }
}

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

/// Wraps an `FFI_ScalarFunction` vtable (from a loaded extension module)
/// into Daft's `ScalarUDF` and `ScalarFunctionFactory` traits.
#[derive(Clone)]
pub struct ScalarFunctionHandle {
    name: &'static str,
    module_path: Option<PathBuf>,
    /// Values of the arguments that folded to constants when this call site was
    /// planned, positionally; `None` where the argument is not a constant.
    ///
    /// Captured once in [`ScalarFunctionFactory::get_function`] and used by both
    /// `get_return_field` and `call` so that planning and execution always agree
    /// on the output field, even though `call` only ever sees arrays.
    literals: Arc<[Option<Literal>]>,
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
        let module_path = Some(module.path().to_path_buf());
        Self {
            name,
            module_path,
            literals: Arc::from([]),
            inner: Some(Arc::new(Inner { ffi, module })),
        }
    }

    /// Returns the inner FFI handle, or an error if deserialized without a loaded extension.
    fn inner(&self) -> DaftResult<&Inner> {
        self.inner.as_deref().ok_or_else(|| {
            DaftError::InternalError(format!("extension function '{}' is not loaded", self.name,))
        })
    }

    /// The literal captured for argument `index`, if any.
    ///
    /// Falls back to "no literal" when the arity does not match what was
    /// captured — a handle can be built for one argument list and reused with
    /// another (SQL registration, for one), and a positional mismatch must not
    /// hand an extension the wrong value.
    fn literal_at(&self, index: usize, arity: usize) -> Option<&Literal> {
        if self.literals.len() != arity {
            return None;
        }
        self.literals[index].as_ref()
    }

    /// Build the argument descriptors passed to the module's `get_return_field`.
    ///
    /// The caller owns the result and must pass it to [`release_descriptors`].
    fn build_descriptors(&self, fields: &[Field]) -> DaftResult<Vec<ArgDescriptor>> {
        let mut descriptors = Vec::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            let schema = match export_field(field) {
                Ok(schema) => schema,
                Err(e) => {
                    // Don't leak the descriptors already built.
                    release_descriptors(&mut descriptors);
                    return Err(e);
                }
            };
            let literal = self
                .literal_at(i, fields.len())
                .and_then(|literal| export_literal(literal, &field.dtype));
            descriptors.push(ArgDescriptor::new(schema, literal));
        }
        Ok(descriptors)
    }

    /// Invoke the module's `get_return_field` for the given argument fields.
    fn ffi_return_field(&self, fields: &[Field]) -> DaftResult<Field> {
        let inner = self.inner()?;
        let mut descriptors = self.build_descriptors(fields)?;

        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            (inner.ffi.get_return_field)(
                inner.ffi.ctx,
                descriptors.as_ptr(),
                descriptors.len(),
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        release_descriptors(&mut descriptors);
        inner.check(rc, errmsg, "unknown error in extension get_return_field")?;

        let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { ret_schema.into_owned() };
        let arrow_field = arrow_schema::Field::try_from(&ffi_schema)
            .map_err(|e| DaftError::InternalError(format!("schema import failed: {e}")))?;
        Field::try_from(&arrow_field)
    }
}

#[typetag::serde]
impl ScalarUDF for ScalarFunctionHandle {
    fn name(&self) -> &'static str {
        self.name
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let fields: Vec<Field> = args
            .into_inner()
            .into_iter()
            .map(|expr| expr.to_field(schema))
            .collect::<DaftResult<_>>()?;

        self.ffi_return_field(&fields)
    }

    fn call(&self, args: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inner = self.inner()?;
        let series_vec: Vec<Series> = args.into_inner();

        // Get expected return field via get_return_field (the call FFI only
        // returns a bare array whose schema has an empty field name). The
        // descriptors carry the literals captured at planning time, so this
        // resolves to the same field the planner saw. Do this before exporting
        // the arguments: ownership of those transfers to the module, so an
        // early return after exporting them would leak.
        let arg_fields: Vec<Field> = series_vec.iter().map(|s| s.field().clone()).collect();
        let ret_daft_field = self.ffi_return_field(&arg_fields)?;

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
        let mut s = serializer.serialize_struct("ScalarFunctionHandle", 3)?;
        s.serialize_field("name", self.name)?;
        s.serialize_field("module_path", &self.module_path)?;
        // Carried to the worker so it resolves the same return field as the
        // driver did.
        s.serialize_field("literals", self.literals.as_ref())?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for ScalarFunctionHandle {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        struct Helper {
            name: String,
            // Optional for forward-compat with older serialized plans that
            // predate this field.
            #[serde(default)]
            module_path: Option<PathBuf>,
            #[serde(default)]
            literals: Vec<Option<Literal>>,
        }
        let h = Helper::deserialize(deserializer)?;
        let literals: Arc<[Option<Literal>]> = Arc::from(h.literals);

        // If the module is loaded in this process, adopt the live handle's FFI
        // vtable so calls work — but keep this call site's captured literals,
        // which the registry entry (one per function, not per call site) has no
        // way to know about.
        if let Some(path) = &h.module_path
            && let Some(existing) =
                crate::module::lookup_extension_function(path, &h.name).map_err(D::Error::custom)?
        {
            return Ok(Self {
                literals,
                ..existing
            });
        }

        // Fall back to a handle with `inner: None`. Calls will error with
        // `extension function 'X' is not loaded`, which is the correct
        // behavior when the worker hasn't loaded the extension.
        let name: &'static str = Box::leak(h.name.into_boxed_str());
        Ok(Self {
            name,
            module_path: h.module_path,
            literals,
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
        args: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<BuiltinScalarFnVariant> {
        // Capture which arguments fold to constants for this call site. This is
        // the only point where the argument *expressions* are in hand; `call`
        // sees arrays only.
        let literals: Arc<[Option<Literal>]> = args
            .into_inner()
            .iter()
            .map(|expr| expr.as_literal().cloned())
            .collect();

        Ok(BuiltinScalarFnVariant::Sync(Arc::new(Self {
            literals,
            ..self.clone()
        })))
    }
}

/// Create a [`ScalarFunctionFactory`] from an `FFI_ScalarFunction` vtable.
///
/// Called by the extension loader when a module invokes `define_function`
/// during initialization. As a side effect, this also registers the function
/// in `MODULES` (via [`crate::module::register_extension_function`]) so that
/// deserialized handles on Ray workers can re-attach their FFI vtable after
/// loading the same `.so`. Errors if the parent module has not been loaded
/// via [`crate::module::load_module`] first.
pub fn into_scalar_function_factory(
    ffi: FFI_ScalarFunction,
    module: Arc<ModuleHandle>,
) -> DaftResult<Arc<dyn ScalarFunctionFactory>> {
    let handle = ScalarFunctionHandle::new(ffi, module);
    let name = handle.name.to_string();
    // `module_path` is always `Some` for freshly constructed handles.
    if let Some(path) = &handle.module_path {
        crate::module::register_extension_function(path, name, handle.clone())?;
    }
    Ok(Arc::new(handle))
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
        _args: *const ArgDescriptor,
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
        // which in a real flow populates MODULES. We populate it directly here
        // with a unique path so the test doesn't interact with other tests.
        let (ffi, module) = make_mock_handle();
        let path =
            std::path::PathBuf::from("/mock/test_serde_roundtrip_with_module_loaded/mock_module");
        // Override the module's path for this test by constructing a fresh
        // ModuleHandle with the same FFI_Module but our unique path.
        let module = Arc::new(ModuleHandle::new(*module.ffi_module(), path.clone()));
        let live_handle = ScalarFunctionHandle::new(ffi, module);
        crate::module::insert_test_module(path.clone());
        crate::module::register_extension_function(
            &path,
            "increment".to_string(),
            live_handle.clone(),
        )
        .unwrap();

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
            "expected inner to be re-attached from MODULES"
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
        crate::module::insert_test_module(module.path().to_path_buf());
        let factory = into_scalar_function_factory(ffi, module).unwrap();
        assert_eq!(factory.name(), "increment");
    }

    #[test]
    fn test_factory_errors_when_module_not_loaded() {
        // Establish the new invariant: registering a function before
        // load_module has populated MODULES must surface an error rather
        // than silently dropping the registration.
        let (ffi, module) = make_mock_handle();
        let path = std::path::PathBuf::from(
            "/mock/test_factory_errors_when_module_not_loaded/mock_module",
        );
        let module = Arc::new(ModuleHandle::new(*module.ffi_module(), path));
        match into_scalar_function_factory(ffi, module) {
            Ok(_) => panic!("expected registration to fail"),
            Err(e) => assert!(
                e.to_string().contains("not loaded"),
                "unexpected error: {e}"
            ),
        }
    }

    // --- mock that asserts incoming schema carries extension metadata ---

    use std::sync::atomic::{AtomicBool, Ordering};

    static METADATA_SEEN: AtomicBool = AtomicBool::new(false);

    unsafe extern "C" fn meta_mock_name(_ctx: *const c_void) -> *const c_char {
        c"meta_passthrough".as_ptr()
    }

    unsafe extern "C" fn meta_mock_get_return_field(
        _ctx: *const c_void,
        _args: *const ArgDescriptor,
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

    // --- mock whose output field depends on a literal argument's value ---

    /// Reads the single i64 out of a length-1 literal array.
    unsafe fn read_literal_i64(array: &ArrowArray) -> i64 {
        unsafe {
            let bufs = std::slice::from_raw_parts(array.buffers.cast_const(), 2);
            *bufs[1].cast::<i64>().add(array.offset as usize)
        }
    }

    unsafe extern "C" fn width_mock_name(_ctx: *const c_void) -> *const c_char {
        c"width".as_ptr()
    }

    /// Names its output `width_<n>` after the value of argument 1, which must
    /// be a foldable constant.
    unsafe extern "C" fn width_mock_get_return_field(
        _ctx: *const c_void,
        args: *const ArgDescriptor,
        args_count: usize,
        ret: *mut ArrowSchema,
        errmsg: *mut *mut c_char,
    ) -> c_int {
        let descriptors = unsafe { std::slice::from_raw_parts(args, args_count) };
        let Some(literal) = descriptors.get(1).and_then(ArgDescriptor::literal) else {
            let msg = CString::new("width: argument 1 must be a literal").unwrap();
            unsafe { std::ptr::write(errmsg, msg.into_raw()) };
            return 1;
        };
        let width = unsafe { read_literal_i64(literal) };

        let field = arrow_schema::Field::new(
            format!("width_{width}"),
            arrow_schema::DataType::Int64,
            false,
        );
        let schema: ArrowSchema = unsafe {
            ArrowSchema::from_owned(arrow::ffi::FFI_ArrowSchema::try_from(&field).unwrap())
        };
        unsafe { std::ptr::write(ret, schema) };
        0
    }

    unsafe extern "C" fn width_mock_call(
        _ctx: *const c_void,
        args: *const ArrowArray,
        args_schemas: *const ArrowSchema,
        args_count: usize,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        // Take ownership of every argument so nothing leaks.
        let mut len = 0usize;
        for i in 0..args_count {
            let abi_array = unsafe { std::ptr::read(args.add(i)) };
            let abi_schema = unsafe { std::ptr::read(args_schemas.add(i)) };
            let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { abi_array.into_owned() };
            let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { abi_schema.into_owned() };
            let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }.unwrap();
            if i == 0 {
                len = data.len();
            }
        }

        let output: arrow_array::ArrayRef =
            Arc::new(arrow_array::Int64Array::from(vec![0i64; len]));
        let (ffi_arr, ffi_sch) = arrow::ffi::to_ffi(&output.to_data()).unwrap();
        unsafe {
            std::ptr::write(ret_array, ArrowArray::from_owned(ffi_arr));
            std::ptr::write(ret_schema, ArrowSchema::from_owned(ffi_sch));
        }
        0
    }

    fn make_width_factory() -> ScalarFunctionHandle {
        let ctx = Box::into_raw(Box::new(IncrementCtx));
        let ffi = FFI_ScalarFunction {
            ctx: ctx.cast(),
            name: width_mock_name,
            get_return_field: width_mock_get_return_field,
            call: width_mock_call,
            fini: mock_fini,
        };
        ScalarFunctionHandle::new(ffi, make_mock_module_handle())
    }

    #[test]
    fn test_return_field_sees_literal_value() {
        let factory = make_width_factory();
        let args =
            FunctionArgs::new_unnamed(vec![daft_dsl::resolved_col("x"), daft_dsl::lit(3i64)]);
        let udf =
            ScalarFunctionFactory::get_function(&factory, args.clone(), &Schema::empty()).unwrap();
        let BuiltinScalarFnVariant::Sync(udf) = udf else {
            panic!("expected a sync function");
        };

        let schema = Schema::new(vec![Field::new("x", DataType::Int64)]);
        let field = udf.get_return_field(args, &schema).unwrap();
        assert_eq!(field.name.as_ref(), "width_3");
    }

    #[test]
    fn test_return_field_errors_when_argument_is_not_foldable() {
        let factory = make_width_factory();
        let args = FunctionArgs::new_unnamed(vec![
            daft_dsl::resolved_col("x"),
            daft_dsl::resolved_col("y"),
        ]);
        let udf =
            ScalarFunctionFactory::get_function(&factory, args.clone(), &Schema::empty()).unwrap();
        let BuiltinScalarFnVariant::Sync(udf) = udf else {
            panic!("expected a sync function");
        };

        let schema = Schema::new(vec![
            Field::new("x", DataType::Int64),
            Field::new("y", DataType::Int64),
        ]);
        let err = udf.get_return_field(args, &schema).unwrap_err();
        assert!(err.to_string().contains("must be a literal"), "{err}");
    }

    #[test]
    fn test_call_resolves_same_field_as_planning() {
        let factory = make_width_factory();
        let args =
            FunctionArgs::new_unnamed(vec![daft_dsl::resolved_col("x"), daft_dsl::lit(4i64)]);
        let udf = ScalarFunctionFactory::get_function(&factory, args, &Schema::empty()).unwrap();
        let BuiltinScalarFnVariant::Sync(udf) = udf else {
            panic!("expected a sync function");
        };

        // `call` only ever sees arrays, so the literal has to come from what was
        // captured at planning time.
        let column: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]));
        let column = Series::from_arrow(Field::new("x", DataType::Int64), column).unwrap();
        let literal = Series::from_literals(vec![Literal::Int64(4)]).unwrap();

        let args = FunctionArgs::new_unnamed(vec![column, literal]);
        let ctx = EvalContext { row_count: 3 };
        let result = udf.call(args, &ctx).unwrap();
        assert_eq!(result.field().name.as_ref(), "width_4");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_serde_preserves_captured_literals() {
        // Simulate the driver → worker hop: the worker's registry entry is the
        // one built at load time and carries no literals, so the call site's
        // literals have to survive serialization.
        let path = std::path::PathBuf::from("/mock/test_serde_preserves_captured_literals/mock");
        let registry_entry = ScalarFunctionHandle {
            module_path: Some(path.clone()),
            ..make_width_factory()
        };
        crate::module::insert_test_module(path.clone());
        crate::module::register_extension_function(
            &path,
            "width".to_string(),
            registry_entry.clone(),
        )
        .unwrap();

        // Driver: a planned call site carrying a captured literal.
        let planned = ScalarFunctionHandle {
            literals: Arc::from([None, Some(Literal::Int64(7))]),
            ..registry_entry.clone()
        };
        let json = serde_json::to_string(&planned).unwrap();

        // Worker: the registry supplies the vtable, the plan supplies literals.
        let deser: ScalarFunctionHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.literals.to_vec(), vec![None, Some(Literal::Int64(7))]);
        assert!(deser.inner.is_some(), "expected the vtable to re-attach");

        let args =
            FunctionArgs::new_unnamed(vec![daft_dsl::resolved_col("x"), daft_dsl::lit(7i64)]);
        let schema = Schema::new(vec![Field::new("x", DataType::Int64)]);
        assert_eq!(
            deser.get_return_field(args, &schema).unwrap().name.as_ref(),
            "width_7"
        );
    }

    #[test]
    fn extension_function_registry_roundtrip() {
        let (ffi, module) = make_mock_handle();
        let path = module.path().to_path_buf();
        let handle = ScalarFunctionHandle::new(ffi, module);
        crate::module::insert_test_module(path.clone());
        crate::module::register_extension_function(&path, "increment".to_string(), handle).unwrap();

        let found = crate::module::lookup_extension_function(&path, "increment")
            .unwrap()
            .expect("should find registered function");
        assert_eq!(ScalarUDF::name(&found), "increment");
        assert!(found.inner.is_some());
    }
}
