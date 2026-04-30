use std::sync::Arc;

#[cfg(not(feature = "python"))]
use crate::catalog::error::CatalogError;
use crate::catalog::{FunctionRef, error::CatalogResult};

/// A Python UDF class (daft.udf.py:UDF).
type PythonScalarFunction = daft_dsl::functions::python::WrappedUDFClass;

/// A native scalar function factory from an extension.
type NativeScalarFunction = Arc<dyn daft_dsl::functions::ScalarFunctionFactory>;

/// A function registered in a session, either a Python UDF or a native extension function.
#[derive(Clone)]
pub enum ScalarFunction {
    /// A Python UDF class (daft.udf.py:UDF).
    Python(PythonScalarFunction),
    /// A native scalar function factory from an extension.
    Native(NativeScalarFunction),
}

impl ScalarFunction {
    /// Returns the function name.
    pub fn name(&self) -> CatalogResult<String> {
        match self {
            #[cfg(feature = "python")]
            Self::Python(udf) => Ok(udf.name()?),
            #[cfg(not(feature = "python"))]
            Self::Python(_) => Err(CatalogError::unsupported(
                "Python UDFs require the python feature",
            )),
            Self::Native(factory) => Ok(factory.name().to_string()),
        }
    }

    /// Convert a [`FunctionRef`] into a [`ScalarFunction`].
    ///
    /// Dispatches based on [`Function::is_python`]:
    /// - **Python functions**: wraps via [`Function::to_py`] into a
    ///   [`ScalarFunction::Python`] (a `WrappedUDFClass`).
    /// - **Native functions**: wraps via [`Function::to_scalar_function_factory`]
    ///   into a [`ScalarFunction::Native`].
    pub fn from_function_ref(function_ref: &FunctionRef) -> Self {
        if function_ref.is_python() {
            #[cfg(feature = "python")]
            {
                use daft_dsl::functions::python::WrappedUDFClass;

                pyo3::Python::attach(|py| {
                    let callable = function_ref
                        .to_py(py)
                        .expect("failed to convert catalog Function to Python object");
                    Self::Python(WrappedUDFClass {
                        inner: std::sync::Arc::new(callable),
                    })
                })
            }
            #[cfg(not(feature = "python"))]
            {
                unreachable!("Python functions require the python feature")
            }
        } else {
            Self::Native(function_ref.to_scalar_function_factory())
        }
    }
}

impl std::fmt::Debug for ScalarFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Python(udf) => f.debug_tuple("Python").field(udf).finish(),
            Self::Native(factory) => f.debug_tuple("Native").field(&factory.name()).finish(),
        }
    }
}

impl From<PythonScalarFunction> for ScalarFunction {
    fn from(function: PythonScalarFunction) -> Self {
        Self::Python(function)
    }
}

impl From<NativeScalarFunction> for ScalarFunction {
    fn from(function: NativeScalarFunction) -> Self {
        Self::Native(function)
    }
}
