use std::sync::Arc;

use daft_catalog::error::CatalogResult;

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
            Self::Python(_) => Err(daft_catalog::error::CatalogError::unsupported(
                "Python UDFs require the python feature",
            )),
            Self::Native(factory) => Ok(factory.name().to_string()),
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
