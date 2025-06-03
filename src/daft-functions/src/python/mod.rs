macro_rules! simple_python_wrapper {
    (
        $fn_name:ident
        , $function:path
        , [$($arg:ident: $t:ty),* $(,)?]
        $(,)?
    ) => {
        #[pyfunction]
        pub fn $fn_name($($arg: $t),*) -> PyResult<PyExpr> {
            Ok($function($($arg.into()),*).into())
        }
    };
}

mod coalesce;
mod misc;
mod sequence;
mod tokenize;

use std::sync::Arc;

use daft_core::prelude::Schema;
use daft_dsl::{
    functions::{
        FunctionArg, FunctionArgs, ScalarFunction, ScalarFunctionFactory, FUNCTION_REGISTRY,
    },
    python::PyExpr,
    ExprRef,
};
use pyo3::{
    types::{PyDict, PyModule, PyModuleMethods, PyTuple},
    wrap_pyfunction, Bound, PyResult,
};

#[pyo3::pyclass]
pub struct PyScalarFunction {
    pub inner: Arc<dyn ScalarFunctionFactory>,
}

use pyo3::prelude::*;

#[pyo3::pymethods]
impl PyScalarFunction {
    #[pyo3(signature  = (*py_args, **py_kwargs))]
    pub fn __call__(
        &self,
        py_args: &Bound<'_, PyTuple>,
        py_kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyExpr> {
        let mut inputs = py_args
            .iter()
            .map(|py| {
                py.extract::<PyExpr>()
                    .map(|e| FunctionArg::unnamed(e.into()))
            })
            .collect::<PyResult<Vec<_>>>()?;

        if let Some(kwargs) = py_kwargs {
            for (k, v) in kwargs {
                let key = k.extract::<String>()?;
                let value = v.extract::<PyExpr>()?;

                inputs.push(FunctionArg::named(Arc::from(key), value.expr));
            }
        }

        // Python only uses dynamic function, so we pass an empty schema.
        // Once ExprRef uses the ScalarFunctionFactory, then we can do function
        // resolution in the planner. For now, we'll necessarily get the single dynamic function
        // out of the DynamicScalarFunctionFactory which replicates the behavior of
        // today and covers name resolution in the DSL.
        let schema = Schema::empty();
        let inputs = FunctionArgs::try_new(inputs)?;
        let udf = self.inner.get_function(inputs.clone(), &schema)?;

        let expr: ExprRef = ScalarFunction { udf, inputs }.into();
        Ok(expr.into())
    }
}

/// Lookup a scalar function factory by name.
#[pyo3::pyfunction]
pub fn get_function_from_registry(name: &str) -> PyResult<PyScalarFunction> {
    Ok(FUNCTION_REGISTRY
        .read()
        .unwrap()
        .get(name)
        .map(|inner| PyScalarFunction { inner })
        .expect("Function was missing an implementation"))
}

pub fn register(parent: &Bound<PyModule>) -> PyResult<()> {
    macro_rules! add {
        ($p:path) => {
            parent.add_function(wrap_pyfunction!($p, parent)?)?;
        };
    }
    parent.add_function(wrap_pyfunction!(get_function_from_registry, parent)?)?;

    add!(coalesce::coalesce);

    add!(misc::to_struct);

    add!(sequence::monotonically_increasing_id);

    add!(tokenize::tokenize_encode);
    add!(tokenize::tokenize_decode);

    Ok(())
}
