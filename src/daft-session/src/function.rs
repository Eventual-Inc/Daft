use std::sync::Arc;

use daft_catalog::error::CatalogResult;

/// A Python UDF class (daft.udf.py:UDF).
type PythonFunction = daft_dsl::functions::python::WrappedUDFClass;

/// A native scalar function factory from an extension.
type NativeFunction = Arc<dyn daft_dsl::functions::ScalarFunctionFactory>;

/// A function registered in a session, either a Python UDF or a native extension function.
#[derive(Clone)]
pub enum Function {
    /// A Python UDF class (daft.udf.py:UDF).
    Python(PythonFunction),
    /// A native scalar function factory from an extension.
    Native(NativeFunction),
}

impl Function {
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

impl std::fmt::Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Python(udf) => f.debug_tuple("Python").field(udf).finish(),
            Self::Native(factory) => f.debug_tuple("Native").field(&factory.name()).finish(),
        }
    }
}

impl From<PythonFunction> for Function {
    fn from(function: PythonFunction) -> Self {
        Self::Python(function)
    }
}

impl From<NativeFunction> for Function {
    fn from(function: NativeFunction) -> Self {
        Self::Native(function)
    }
}
