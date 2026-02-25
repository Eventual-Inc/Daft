use std::{
    ffi::{CStr, c_char},
    mem::ManuallyDrop,
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
use daft_ext_abi::{
    FFI_ArrowArray, FFI_ArrowSchema, FFI_ScalarFunction,
    ffi::arrow::{export_arrow_array, import_arrow_array},
};
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

/// Wraps an `FFI_ScalarFunction` vtable (from a loaded extension module)
/// into Daft's `ScalarUDF` and `ScalarFunctionFactory` traits.
#[derive(Clone)]
pub struct ScalarFunctionHandle {
    name: &'static str,
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
        Self {
            name,
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

        let ffi_schemas: Vec<FFI_ArrowSchema> = arrow_fields
            .iter()
            .map(|f| {
                FFI_ArrowSchema::try_from(f)
                    .map_err(|e| DaftError::InternalError(format!("schema export failed: {e}")))
            })
            .collect::<DaftResult<_>>()?;

        let mut ret_schema = FFI_ArrowSchema::empty();
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

        let arrow_field = arrow_schema::Field::try_from(&ret_schema)
            .map_err(|e| DaftError::InternalError(format!("schema import failed: {e}")))?;

        Field::try_from(&arrow_field)
    }

    fn call(&self, args: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inner = self.inner()?;
        let series_vec: Vec<Series> = args.into_inner();

        let mut ffi_arrays: Vec<ManuallyDrop<FFI_ArrowArray>> =
            Vec::with_capacity(series_vec.len());
        let mut ffi_schemas: Vec<FFI_ArrowSchema> = Vec::with_capacity(series_vec.len());

        for s in &series_vec {
            let arrow_arr = s.to_arrow()?;
            let (arr, schema) = export_arrow_array(&arrow_arr).map_err(DaftError::InternalError)?;
            ffi_arrays.push(ManuallyDrop::new(arr));
            ffi_schemas.push(schema);
        }

        // Get expected return field via get_return_field (the call FFI only
        // returns a bare array whose schema has an empty field name).
        let mut ret_field_schema = FFI_ArrowSchema::empty();
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

        let ret_arrow_field = arrow_schema::Field::try_from(&ret_field_schema)
            .map_err(|e| DaftError::InternalError(format!("schema import failed: {e}")))?;
        let ret_daft_field = Field::try_from(&ret_arrow_field)?;

        // Execute the function.
        let mut ret_array = FFI_ArrowArray::empty();
        let mut ret_schema = FFI_ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (inner.ffi.call)(
                inner.ffi.ctx,
                ffi_arrays.as_ptr().cast(),
                ffi_schemas.as_ptr(),
                ffi_arrays.len(),
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "unknown error in extension call")?;

        let result_arr = unsafe { import_arrow_array(ret_array, &ret_schema) }
            .map_err(DaftError::InternalError)?;

        Series::from_arrow(ret_daft_field, result_arr)
    }

    fn docstring(&self) -> &'static str {
        "External extension function"
    }
}

impl Serialize for ScalarFunctionHandle {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("ScalarFunctionHandle", 1)?;
        s.serialize_field("name", self.name)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for ScalarFunctionHandle {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            name: String,
        }
        let h = Helper::deserialize(deserializer)?;
        Ok(Self {
            name: Box::leak(h.name.into_boxed_str()),
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
/// during initialization.
pub fn into_scalar_function_factory(
    ffi: FFI_ScalarFunction,
    module: Arc<ModuleHandle>,
) -> Arc<dyn ScalarFunctionFactory> {
    Arc::new(ScalarFunctionHandle::new(ffi, module))
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
        _args: *const FFI_ArrowSchema,
        _args_count: usize,
        ret: *mut FFI_ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        let field = arrow_schema::Field::new("result", arrow_schema::DataType::Int32, false);
        let schema = FFI_ArrowSchema::try_from(&field).unwrap();
        unsafe { std::ptr::write(ret, schema) };
        0
    }

    unsafe extern "C" fn mock_call(
        _ctx: *const c_void,
        args: *const FFI_ArrowArray,
        args_schemas: *const FFI_ArrowSchema,
        args_count: usize,
        ret_array: *mut FFI_ArrowArray,
        ret_schema: *mut FFI_ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        assert_eq!(args_count, 1);
        let ffi_array = unsafe { std::ptr::read(args) };
        let ffi_schema = unsafe { &*args_schemas };
        let data = unsafe { arrow::ffi::from_ffi(ffi_array, ffi_schema) }.unwrap();
        let input = arrow_array::make_array(data)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array")
            .clone();

        let output: Int32Array = input.iter().map(|v| v.map(|x| x + 1)).collect();
        let output_ref: arrow_array::ArrayRef = Arc::new(output);
        let (out_array, out_schema) = arrow::ffi::to_ffi(&output_ref.to_data()).unwrap();
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

    unsafe extern "C" fn mock_init(_session: *mut daft_ext_abi::FFI_SessionContext) -> c_int {
        0
    }

    fn make_mock_module_handle() -> Arc<ModuleHandle> {
        use daft_ext_abi::FFI_Module;
        let module = FFI_Module {
            daft_abi_version: daft_ext_abi::DAFT_ABI_VERSION,
            name: c"mock_module".as_ptr(),
            init: mock_init,
            free_string: mock_free_string,
        };
        Arc::new(ModuleHandle::new(module))
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
        assert_eq!(field.name.as_str(), "result");
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
    fn test_serde_roundtrip() {
        let (ffi, module) = make_mock_handle();
        let udf = ScalarFunctionHandle::new(ffi, module);

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
    fn test_factory() {
        let (ffi, module) = make_mock_handle();
        let factory = into_scalar_function_factory(ffi, module);
        assert_eq!(factory.name(), "increment");
    }
}
