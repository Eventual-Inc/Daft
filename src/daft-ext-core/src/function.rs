use std::{
    ffi::{CStr, c_char, c_int, c_void},
    sync::Arc,
};

use arrow_array::ArrayRef;
use arrow_schema::Field;
use daft_ext_abi::{FFI_ArrowArray, FFI_ArrowSchema, FFI_ScalarFunction};

use crate::{
    error::DaftResult,
    ffi::{
        arrow::{export_arrow_result, import_arrow_args},
        strings::{from_json_cstr, to_json_cstr},
        trampoline::trampoline,
    },
};

/// Trait that extension authors implement to define a scalar function.
pub trait DaftScalarFunction {
    fn name(&self) -> &CStr;
    fn return_field(&self, args: &[Field]) -> DaftResult<Field>;
    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef>;
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
    ctx:       *const c_void,
    args_json: *const c_char,
    ret_json:  *mut *mut c_char,
    errmsg:    *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in get_return_field", || {
        let ctx = &*ctx.cast::<DaftScalarFunctionRef>();
        let fields: Vec<Field> = from_json_cstr(args_json)?;
        let result = ctx.return_field(&fields)?;
        *ret_json = to_json_cstr(&result)?;
        Ok(())
    })}
}

/// Evaluates the function on Arrow arrays via the C Data Interface.
#[rustfmt::skip]
unsafe extern "C" fn ffi_call(
    ctx:          *const c_void,
    args:         *const FFI_ArrowArray,
    args_schemas: *const FFI_ArrowSchema,
    args_count:   usize,
    ret_array:    *mut FFI_ArrowArray,
    ret_schema:   *mut FFI_ArrowSchema,
    errmsg:       *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in call", || {
        let ctx = &*ctx.cast::<DaftScalarFunctionRef>();
        let arrays = import_arrow_args(args, args_schemas, args_count)?;
        let result = ctx.call(&arrays)?;
        export_arrow_result(result, ret_array, ret_schema)
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
    use std::ffi::CString;

    use arrow::ffi as arrow_ffi;
    use arrow_array::{Array, Int32Array, make_array};
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::{error::DaftError, ffi::strings::free_string};

    struct IncrementFn;

    impl DaftScalarFunction for IncrementFn {
        fn name(&self) -> &CStr {
            c"increment"
        }

        fn return_field(&self, _args: &[Field]) -> DaftResult<Field> {
            Ok(Field::new("result", DataType::Int32, false))
        }

        fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
            let input = args[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DaftError::TypeError("expected Int32".into()))?;
            let output: Int32Array = input.iter().map(|v| v.map(|x| x + 1)).collect();
            Ok(Arc::new(output))
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

        let fields = vec![Field::new("x", DataType::Int32, false)];
        let args_json = CString::new(serde_json::to_string(&fields).unwrap()).unwrap();

        let mut ret_json: *mut c_char = std::ptr::null_mut();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                args_json.as_ptr(),
                &raw mut ret_json,
                &raw mut errmsg,
            )
        };

        assert_eq!(rc, 0, "get_return_field should succeed");
        assert!(!ret_json.is_null());

        let result_str = unsafe { CStr::from_ptr(ret_json) }.to_str().unwrap();
        let result_field: Field = serde_json::from_str(result_str).unwrap();
        assert_eq!(result_field.name(), "result");
        assert_eq!(*result_field.data_type(), DataType::Int32);

        unsafe { free_string(ret_json) };
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_call_roundtrip() {
        use std::mem::ManuallyDrop;

        let vtable = into_ffi(Arc::new(IncrementFn));

        let input = Int32Array::from(vec![1, 2, 3]);
        let (ffi_array, ffi_schema) = arrow_ffi::to_ffi(&input.to_data()).unwrap();
        let ffi_array = ManuallyDrop::new(ffi_array);

        let mut ret_array = FFI_ArrowArray::empty();
        let mut ret_schema = FFI_ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.call)(
                vtable.ctx,
                &raw const *ffi_array,
                &raw const ffi_schema,
                1,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };

        assert_eq!(rc, 0, "call should succeed");

        let result_data = unsafe { arrow_ffi::from_ffi(ret_array, &ret_schema) }.unwrap();
        let result = make_array(result_data);
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
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
            fn return_field(&self, _: &[Field]) -> DaftResult<Field> {
                Err(DaftError::TypeError("bad type".into()))
            }
            fn call(&self, _: &[ArrayRef]) -> DaftResult<ArrayRef> {
                Err(DaftError::RuntimeError("compute failed".into()))
            }
        }

        let vtable = into_ffi(Arc::new(FailingFn));

        let args_json = CString::new("[]").unwrap();
        let mut ret_json: *mut c_char = std::ptr::null_mut();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                args_json.as_ptr(),
                &raw mut ret_json,
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
    fn fini_is_callable() {
        struct DisposableFn;
        impl DaftScalarFunction for DisposableFn {
            fn name(&self) -> &CStr {
                c"disposable"
            }
            fn return_field(&self, _: &[Field]) -> DaftResult<Field> {
                Ok(Field::new("x", DataType::Null, true))
            }
            fn call(&self, _: &[ArrayRef]) -> DaftResult<ArrayRef> {
                Ok(Arc::new(Int32Array::from(vec![0])))
            }
        }

        let vtable = into_ffi(Arc::new(DisposableFn));
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }
}
