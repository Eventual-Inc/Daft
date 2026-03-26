use std::{
    ffi::{CStr, c_char, c_int, c_void},
    sync::Arc,
};

use crate::{
    abi::{ArrowArray, ArrowData, ArrowSchema, FFI_ScalarFunction},
    error::DaftResult,
    ffi::trampoline::trampoline,
};

/// Trait that extension authors implement to define a scalar function.
pub trait DaftScalarFunction {
    fn name(&self) -> &CStr;
    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema>;
    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData>;
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
        let mut data = Vec::with_capacity(args_count);
        for i in 0..args_count {
            let array = std::ptr::read(args.add(i));
            let schema = std::ptr::read(args_schemas.add(i));
            data.push(ArrowData { schema, array });
        }
        let result = ctx.call(data)?;
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

#[cfg(all(test, feature = "arrow-57"))]
mod tests {
    use arrow_schema_57::{DataType, Field, Schema};

    use super::*;
    use crate::{abi::ffi::strings::free_string, error::DaftError};

    // ── Raw-level test helpers (no arrow-array dependency) ──────────

    /// Create an [`ArrowSchema`] from an `arrow_schema_57::Schema`.
    fn export_schema(schema: &Schema) -> ArrowSchema {
        let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(schema).unwrap();
        unsafe { ArrowSchema::from_owned(ffi) }
    }

    /// Read an [`ArrowSchema`] back into an `arrow_schema_57::Schema`.
    fn import_schema(schema: &ArrowSchema) -> Schema {
        let ffi: &arrow_schema_57::ffi::FFI_ArrowSchema = unsafe { schema.as_raw() };
        Schema::try_from(ffi).unwrap()
    }

    /// Build an [`ArrowData`] containing an Int32 column from raw values.
    ///
    /// Allocates buffers on the heap; the release callback frees them.
    fn make_int32(values: &[i32]) -> ArrowData {
        let schema = {
            let field = Field::new("", DataType::Int32, false);
            let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(&field).unwrap();
            unsafe { ArrowSchema::from_owned(ffi) }
        };

        let values: Box<[i32]> = values.into();
        let len = values.len();

        // Arrow Int32 layout: [validity (null), values]
        let buffers = Box::new([std::ptr::null::<c_void>(), values.as_ptr().cast::<c_void>()]);

        // Pack both allocations into private_data so `release` can free them.
        let private = Box::new((values, std::ptr::null::<c_void>()));
        let array = ArrowArray {
            length: len as i64,
            null_count: 0,
            offset: 0,
            n_buffers: 2,
            n_children: 0,
            buffers: Box::into_raw(buffers).cast::<*const c_void>(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release_int32),
            private_data: Box::into_raw(private).cast::<c_void>(),
        };

        ArrowData { schema, array }
    }

    unsafe extern "C" fn release_int32(array: *mut ArrowArray) {
        let a = unsafe { &mut *array };
        // Free the (values, _) tuple.
        drop(unsafe { Box::from_raw(a.private_data.cast::<(Box<[i32]>, *const c_void)>()) });
        // Free the buffers pointer array.
        drop(unsafe { Box::from_raw(a.buffers.cast::<[*const c_void; 2]>()) });
        a.release = None;
    }

    /// Read the i32 values out of an [`ArrowData`] (non-null, zero-offset).
    fn read_int32(data: &ArrowData) -> &[i32] {
        unsafe {
            let bufs = std::slice::from_raw_parts(data.array.buffers.cast_const(), 2);
            std::slice::from_raw_parts(bufs[1].cast::<i32>(), data.array.length as usize)
        }
    }

    // ── Test function impls ─────────────────────────────────────────

    struct IncrementFn;

    impl DaftScalarFunction for IncrementFn {
        fn name(&self) -> &CStr {
            c"increment"
        }

        fn return_field(&self, _args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
            let field = Field::new("result", DataType::Int32, false);
            Ok(export_schema(&Schema::new(vec![field])))
        }

        fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
            let input = args
                .first()
                .ok_or_else(|| DaftError::TypeError("expected at least one argument".into()))?;
            let values = read_int32(input);
            let output: Vec<i32> = values.iter().map(|x| x + 1).collect();
            Ok(make_int32(&output))
        }
    }

    // ── Tests ───────────────────────────────────────────────────────

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
        let ffi_schema = export_schema(&Schema::new(vec![field]));

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

        let schema = import_schema(&ret_schema);
        assert_eq!(schema.field(0).name(), "result");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_call_roundtrip() {
        let vtable = into_ffi(Arc::new(IncrementFn));

        let data = make_int32(&[1, 2, 3]);

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

        let result = ArrowData {
            schema: ret_schema,
            array: ret_array,
        };
        assert_eq!(read_int32(&result), &[2, 3, 4]);

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
            fn call(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
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
                Ok(export_schema(&Schema::new(vec![Field::new(
                    "x",
                    DataType::Int32,
                    false,
                )])))
            }
            fn call(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Err(DaftError::RuntimeError("compute failed".into()))
            }
        }

        let vtable = into_ffi(Arc::new(CallFailFn));

        let data = make_int32(&[1]);

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
                Ok(export_schema(&Schema::new(vec![Field::new(
                    "result",
                    DataType::Int32,
                    false,
                )])))
            }
            fn call(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Ok(make_int32(&[42]))
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

        let schema = import_schema(&ret_schema);
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
                Ok(export_schema(&Schema::new(vec![Field::new(
                    "x",
                    DataType::Null,
                    true,
                )])))
            }
            fn call(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Ok(make_int32(&[0]))
            }
        }

        let vtable = into_ffi(Arc::new(DisposableFn));
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }
}
