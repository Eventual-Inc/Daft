use std::ffi::CStr;

use crate::{
    abi::{ArrowSchema, FFI_ExtensionType, FFI_SessionContext},
    function::{DaftScalarFunctionRef, into_ffi},
};

/// Trait for installing an extension within a session.
pub trait DaftSession {
    fn define_function(&mut self, function: DaftScalarFunctionRef);
    fn define_type(&mut self, name: &CStr, storage_schema: ArrowSchema);
}

/// Trait implemented by extension crates to install themselves.
pub trait DaftExtension {
    fn install(session: &mut dyn DaftSession);
}

/// A [`DaftSession`] backed by a [`FFI_SessionContext`] from the host.
///
/// This bridges the safe `DaftSession` trait to the C ABI session
/// context provided by the Daft host.
pub struct SessionContext<'a> {
    session: &'a mut FFI_SessionContext,
}

impl<'a> SessionContext<'a> {
    pub fn new(session: &'a mut FFI_SessionContext) -> Self {
        Self { session }
    }
}

impl DaftSession for SessionContext<'_> {
    fn define_function(&mut self, func: DaftScalarFunctionRef) {
        let vtable = into_ffi(func);
        let rc = unsafe { (self.session.define_function)(self.session.ctx, vtable) };
        assert_eq!(rc, 0, "host define_function returned non-zero: {rc}");
    }

    fn define_type(&mut self, name: &CStr, storage_schema: ArrowSchema) {
        let ffi_type = FFI_ExtensionType {
            name: name.as_ptr(),
            storage_schema,
        };
        let rc = unsafe { (self.session.define_type)(self.session.ctx, ffi_type) };
        assert_eq!(rc, 0, "host define_type returned non-zero: {rc}");
    }
}

#[cfg(all(test, feature = "arrow-57"))]
mod tests {
    use std::{
        ffi::{CStr, c_int, c_void},
        sync::{Arc, Mutex},
    };

    use arrow_schema_57::{DataType, Field, Schema};

    use super::*;
    use crate::{
        abi::{ArrowData, ArrowSchema, FFI_ScalarFunction},
        error::DaftResult,
        function::DaftScalarFunction,
    };

    fn export_schema(schema: &Schema) -> ArrowSchema {
        let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(schema).unwrap();
        unsafe { ArrowSchema::from_owned(ffi) }
    }

    /// Minimal stub [`ArrowData`] for tests that never inspect the array.
    fn stub_int32() -> ArrowData {
        let schema = {
            let field = Field::new("", DataType::Int32, false);
            let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(&field).unwrap();
            unsafe { ArrowSchema::from_owned(ffi) }
        };
        ArrowData {
            schema,
            array: crate::abi::ArrowArray::empty(),
        }
    }

    #[test]
    fn session_context_integration() {
        static RECORDED: Mutex<Vec<String>> = Mutex::new(Vec::new());

        unsafe extern "C" fn mock_define(_ctx: *mut c_void, func: FFI_ScalarFunction) -> c_int {
            let name = unsafe { CStr::from_ptr((func.name)(func.ctx)) }
                .to_str()
                .unwrap()
                .to_string();
            RECORDED.lock().unwrap().push(name);
            unsafe { (func.fini)(func.ctx.cast_mut()) };
            0
        }

        unsafe extern "C" fn mock_define_type(
            _ctx: *mut c_void,
            _ext_type: FFI_ExtensionType,
        ) -> c_int {
            0
        }

        let mut raw_session = FFI_SessionContext {
            ctx: std::ptr::null_mut(),
            define_function: mock_define,
            define_type: mock_define_type,
        };

        let mut session = SessionContext::new(&mut raw_session);

        struct AddFn;
        impl DaftScalarFunction for AddFn {
            fn name(&self) -> &CStr {
                c"my_add"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                Ok(export_schema(&Schema::new(vec![Field::new(
                    "sum",
                    DataType::Int32,
                    false,
                )])))
            }
            fn call(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Ok(stub_int32())
            }
        }

        session.define_function(Arc::new(AddFn));

        let recorded = RECORDED.lock().unwrap();
        assert!(
            recorded.contains(&"my_add".to_string()),
            "expected 'my_add' in recorded functions"
        );
    }

    #[test]
    fn session_context_define_type() {
        static TYPES: Mutex<Vec<String>> = Mutex::new(Vec::new());

        unsafe extern "C" fn mock_define_fn(_ctx: *mut c_void, _func: FFI_ScalarFunction) -> c_int {
            0
        }

        unsafe extern "C" fn mock_define_type(
            _ctx: *mut c_void,
            ext_type: FFI_ExtensionType,
        ) -> c_int {
            let name = unsafe { CStr::from_ptr(ext_type.name) }
                .to_str()
                .unwrap()
                .to_string();
            TYPES.lock().unwrap().push(name);
            0
        }

        let mut raw_session = FFI_SessionContext {
            ctx: std::ptr::null_mut(),
            define_function: mock_define_fn,
            define_type: mock_define_type,
        };

        let mut session = SessionContext::new(&mut raw_session);

        let field = Field::new("item", DataType::Float64, false);
        let storage = ArrowSchema::try_from(&Field::new(
            "point2d",
            DataType::FixedSizeList(Arc::new(field), 2),
            true,
        ))
        .unwrap();

        session.define_type(c"daft_geo.point2d", storage);

        let types = TYPES.lock().unwrap();
        assert!(types.contains(&"daft_geo.point2d".to_string()));
    }

    #[test]
    fn session_context_multiple_functions() {
        static NAMES: Mutex<Vec<String>> = Mutex::new(Vec::new());

        unsafe extern "C" fn mock_define(_ctx: *mut c_void, func: FFI_ScalarFunction) -> c_int {
            let name = unsafe { CStr::from_ptr((func.name)(func.ctx)) }
                .to_str()
                .unwrap()
                .to_string();
            NAMES.lock().unwrap().push(name);
            unsafe { (func.fini)(func.ctx.cast_mut()) };
            0
        }

        unsafe extern "C" fn mock_define_type(
            _ctx: *mut c_void,
            _ext_type: FFI_ExtensionType,
        ) -> c_int {
            0
        }

        let mut raw_session = FFI_SessionContext {
            ctx: std::ptr::null_mut(),
            define_function: mock_define,
            define_type: mock_define_type,
        };

        let mut session = SessionContext::new(&mut raw_session);

        struct FnA;
        impl DaftScalarFunction for FnA {
            fn name(&self) -> &CStr {
                c"fn_a"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                Ok(export_schema(&Schema::new(vec![Field::new(
                    "a",
                    DataType::Int32,
                    false,
                )])))
            }
            fn call(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Ok(stub_int32())
            }
        }

        struct FnB;
        impl DaftScalarFunction for FnB {
            fn name(&self) -> &CStr {
                c"fn_b"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                Ok(export_schema(&Schema::new(vec![Field::new(
                    "b",
                    DataType::Utf8,
                    true,
                )])))
            }
            fn call(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Ok(stub_int32())
            }
        }

        session.define_function(Arc::new(FnA));
        session.define_function(Arc::new(FnB));

        let names = NAMES.lock().unwrap();
        assert!(names.contains(&"fn_a".to_string()));
        assert!(names.contains(&"fn_b".to_string()));
    }
}
