use std::{
    ffi::{CStr, c_char, c_int, c_void},
    sync::Arc,
};

use daft_ext_abi::{ArrowArray, ArrowData, ArrowSchema, FFI_ScalarFunction};

use crate::{error::DaftResult, ffi::trampoline::trampoline};

/// RAII guard that releases any unconsumed `ArrowData` slots on drop.
///
/// `ArrowArray` and `ArrowSchema` intentionally have no `Drop` impl, so when
/// the trampoline copies host arrays via `ptr::read`, the release callbacks
/// must be invoked manually. On the success path, `ArrowData::take_arg` zeros
/// consumed slots (setting `release` to `None`), so the guard is a no-op for
/// those. On error or panic, the guard releases any remaining live slots.
struct ArrowArgsGuard(Vec<ArrowData>);

impl Drop for ArrowArgsGuard {
    fn drop(&mut self) {
        for slot in &self.0 {
            unsafe {
                if let Some(release) = slot.array.release {
                    release((&raw const slot.array).cast_mut());
                }
                if let Some(release) = slot.schema.release {
                    release((&raw const slot.schema).cast_mut());
                }
            }
        }
    }
}

/// Trait that extension authors implement to define a scalar function.
pub trait DaftScalarFunction {
    fn name(&self) -> &CStr;
    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema>;
    fn call(&self, args: &[ArrowData]) -> DaftResult<ArrowData>;
}

/// A shared, type-erased scalar function reference.
pub type DaftScalarFunctionRef = Arc<dyn DaftScalarFunction>;

/// Convert a [`DaftScalarFunctionRef`] into a [`FFI_ScalarFunction`] vtable.
///
/// The `Arc` is moved into the vtable's opaque context and released
/// when the host calls `fini`.
pub fn into_ffi(func: DaftScalarFunctionRef) -> FFI_ScalarFunction {
    let ctx_ptr = Box::into_raw(Box::new(func));
    FFI_ScalarFunction {
        ctx: ctx_ptr.cast(),
        name: ffi_name,
        get_return_field: ffi_get_return_field,
        call: ffi_call,
        fini: ffi_fini,
    }
}

/// Returns the function name as a null-terminated UTF-8 string.
unsafe extern "C" fn ffi_name(ctx: *const c_void) -> *const c_char {
    unsafe { &*ctx.cast::<DaftScalarFunctionRef>() }
        .name()
        .as_ptr()
}

/// Returns the output field given the input fields.
#[rustfmt::skip]
unsafe extern "C" fn ffi_get_return_field(
    ctx:        *const c_void,
    args:       *const ArrowSchema,
    args_count: usize,
    ret:        *mut ArrowSchema,
    errmsg:     *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in get_return_field", || {
        let ctx = &*ctx.cast::<DaftScalarFunctionRef>();
        let schemas = if args_count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(args, args_count)
        };
        let result = ctx.return_field(schemas)?;
        std::ptr::write(ret, result);
        Ok(())
    })}
}

/// Evaluates the function on Arrow arrays via the C Data Interface.
#[rustfmt::skip]
unsafe extern "C" fn ffi_call(
    ctx:          *const c_void,
    args:         *const ArrowArray,
    args_schemas: *const ArrowSchema,
    args_count:   usize,
    ret_array:    *mut ArrowArray,
    ret_schema:   *mut ArrowSchema,
    errmsg:       *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in call", || {
        let ctx = &*ctx.cast::<DaftScalarFunctionRef>();
        // Build ArrowData slices from the raw pointers.
        // The guard ensures release callbacks are invoked for any slots
        // not consumed via `take_arg` (error path, panic, or partial consumption).
        let mut guard = ArrowArgsGuard(Vec::with_capacity(args_count));
        for i in 0..args_count {
            let array = std::ptr::read(args.add(i));
            let schema = std::ptr::read(args_schemas.add(i));
            guard.0.push(ArrowData { schema, array });
        }
        let result = ctx.call(&guard.0)?;
        std::ptr::write(ret_array, result.array);
        std::ptr::write(ret_schema, result.schema);
        Ok(())
    })}
}

/// Finalizes the function, freeing all owned resources.
unsafe extern "C" fn ffi_fini(ctx: *mut c_void) {
    let _ = std::panic::catch_unwind(|| unsafe {
        drop(Box::from_raw(ctx.cast::<DaftScalarFunctionRef>()));
    });
}

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field};
    use daft_ext_abi::ffi::strings::free_string;

    use super::*;
    use crate::{arrow, error::DaftError};

    struct IncrementFn;

    impl DaftScalarFunction for IncrementFn {
        fn name(&self) -> &CStr {
            c"increment"
        }

        fn return_field(&self, _args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
            let field = Field::new("result", DataType::Int32, false);
            arrow::export_schema(&arrow_schema::Schema::new(vec![field]))
        }

        fn call(&self, args: &[ArrowData]) -> DaftResult<ArrowData> {
            let input_array = arrow::import_array(unsafe { ArrowData::take_arg(args, 0) })?;
            let input = input_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DaftError::TypeError("expected Int32".into()))?;
            let output: Int32Array = input.iter().map(|v| v.map(|x| x + 1)).collect();
            arrow::export_array(&output)
        }
    }

    #[test]
    fn vtable_name_roundtrip() {
        let vtable = into_ffi(Arc::new(IncrementFn));

        let name = unsafe { CStr::from_ptr((vtable.name)(vtable.ctx)) };
        assert_eq!(name.to_str().unwrap(), "increment");

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_get_return_field_roundtrip() {
        let vtable = into_ffi(Arc::new(IncrementFn));

        let field = Field::new("x", DataType::Int32, false);
        let ffi_schema = arrow::export_schema(&arrow_schema::Schema::new(vec![field])).unwrap();

        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                &raw const ffi_schema,
                1,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };

        assert_eq!(rc, 0, "get_return_field should succeed");

        let schema = arrow::import_schema(&ret_schema).unwrap();
        assert_eq!(schema.field(0).name(), "result");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_call_roundtrip() {
        let vtable = into_ffi(Arc::new(IncrementFn));

        let input = Int32Array::from(vec![1, 2, 3]);
        let data = arrow::export_array(&input).unwrap();

        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.call)(
                vtable.ctx,
                &raw const data.array,
                &raw const data.schema,
                1,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };

        assert_eq!(rc, 0, "call should succeed");

        let result_array = arrow::import_array(ArrowData {
            schema: ret_schema,
            array: ret_array,
        })
        .unwrap();
        let result = result_array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.values(), &[2, 3, 4]);

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_error_propagation() {
        struct FailingFn;
        impl DaftScalarFunction for FailingFn {
            fn name(&self) -> &CStr {
                c"failing"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                Err(DaftError::TypeError("bad type".into()))
            }
            fn call(&self, _: &[ArrowData]) -> DaftResult<ArrowData> {
                Err(DaftError::RuntimeError("compute failed".into()))
            }
        }

        let vtable = into_ffi(Arc::new(FailingFn));

        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                std::ptr::null(),
                0,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };

        assert_ne!(rc, 0, "should return non-zero on error");
        assert!(!errmsg.is_null());

        let err_str = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert!(err_str.contains("bad type"), "error message: {err_str}");

        unsafe { free_string(errmsg) };
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_call_error_propagation() {
        struct CallFailFn;
        impl DaftScalarFunction for CallFailFn {
            fn name(&self) -> &CStr {
                c"call_fail"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                arrow::export_schema(&arrow_schema::Schema::new(vec![Field::new(
                    "x",
                    DataType::Int32,
                    false,
                )]))
            }
            fn call(&self, _: &[ArrowData]) -> DaftResult<ArrowData> {
                Err(DaftError::RuntimeError("compute failed".into()))
            }
        }

        let vtable = into_ffi(Arc::new(CallFailFn));

        let input = Int32Array::from(vec![1]);
        let data = arrow::export_array(&input).unwrap();

        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.call)(
                vtable.ctx,
                &raw const data.array,
                &raw const data.schema,
                1,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };

        assert_ne!(rc, 0, "call should return non-zero on error");
        assert!(!errmsg.is_null());

        let err_str = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert!(
            err_str.contains("compute failed"),
            "error message: {err_str}"
        );

        unsafe { free_string(errmsg) };
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_zero_args() {
        struct NoArgFn;
        impl DaftScalarFunction for NoArgFn {
            fn name(&self) -> &CStr {
                c"no_args"
            }
            fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                assert!(args.is_empty());
                arrow::export_schema(&arrow_schema::Schema::new(vec![Field::new(
                    "result",
                    DataType::Int32,
                    false,
                )]))
            }
            fn call(&self, _: &[ArrowData]) -> DaftResult<ArrowData> {
                let output = Int32Array::from(vec![42]);
                arrow::export_array(&output)
            }
        }

        let vtable = into_ffi(Arc::new(NoArgFn));

        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                std::ptr::null(),
                0,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0, "get_return_field with zero args should succeed");

        let schema = arrow::import_schema(&ret_schema).unwrap();
        assert_eq!(schema.field(0).name(), "result");

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn fini_is_callable() {
        struct DisposableFn;
        impl DaftScalarFunction for DisposableFn {
            fn name(&self) -> &CStr {
                c"disposable"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                arrow::export_schema(&arrow_schema::Schema::new(vec![Field::new(
                    "x",
                    DataType::Null,
                    true,
                )]))
            }
            fn call(&self, _: &[ArrowData]) -> DaftResult<ArrowData> {
                let output = Int32Array::from(vec![0]);
                arrow::export_array(&output)
            }
        }

        let vtable = into_ffi(Arc::new(DisposableFn));
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }
}
