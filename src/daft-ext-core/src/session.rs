use daft_ext_abi::FFI_SessionContext;

use crate::{
    function::{DaftScalarFunctionRef, into_ffi},
    source::{DaftSourceRef, into_ffi_source},
};

/// Trait for installing an extension within a session.
pub trait DaftSession {
    fn define_function(&mut self, function: DaftScalarFunctionRef);
    fn define_source(&mut self, source: DaftSourceRef);
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

    fn define_source(&mut self, source: DaftSourceRef) {
        let vtable = into_ffi_source(source);
        let rc = unsafe { (self.session.define_source)(self.session.ctx, vtable) };
        assert_eq!(rc, 0, "host define_source returned non-zero: {rc}");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::{CStr, c_int, c_void},
        sync::{Arc, Mutex},
    };

    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field};
    use daft_ext_abi::{FFI_ScalarFunction, FFI_ScanSource};

    use super::*;
    use crate::{error::DaftResult, function::DaftScalarFunction};

    unsafe extern "C" fn mock_define_source(_ctx: *mut c_void, source: FFI_ScanSource) -> c_int {
        unsafe { (source.fini)(source.ctx.cast_mut()) };
        0
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

        let mut raw_session = FFI_SessionContext {
            ctx: std::ptr::null_mut(),
            define_function: mock_define,
            define_source: mock_define_source,
        };

        let mut session = SessionContext::new(&mut raw_session);

        struct AddFn;
        impl DaftScalarFunction for AddFn {
            fn name(&self) -> &CStr {
                c"my_add"
            }
            fn return_field(&self, _: &[Field]) -> DaftResult<Field> {
                Ok(Field::new("sum", DataType::Int32, false))
            }
            fn call(&self, _: &[ArrayRef]) -> DaftResult<ArrayRef> {
                Ok(Arc::new(Int32Array::from(vec![0])))
            }
        }

        session.define_function(Arc::new(AddFn));

        let recorded = RECORDED.lock().unwrap();
        assert!(
            recorded.contains(&"my_add".to_string()),
            "expected 'my_add' in recorded functions"
        );
    }
}
