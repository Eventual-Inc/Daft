use std::sync::Arc;

use daft_dsl::{
    functions::{FunctionArg, FunctionArgs, ScalarFunction},
    python::PyExpr,
    ExprRef,
};
use pyo3::{
    types::{PyDict, PyModule, PyModuleMethods, PyTuple},
    wrap_pyfunction, Bound, PyResult,
};

#[pyo3::pyclass]
pub struct PyScalarFunction {
    pub inner: Arc<str>,
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

        let inputs = FunctionArgs::try_new(inputs)?;

        let expr: ExprRef = ScalarFunction::new_from_name(self.inner.clone(), inputs).into();
        Ok(expr.into())
    }
}

/// Lookup a scalar function factory by name.
#[pyo3::pyfunction]
pub fn get_function_from_registry(name: &str) -> PyResult<PyScalarFunction> {
    // Defer validation until planning
    Ok(PyScalarFunction {
        inner: Arc::from(name.to_string()),
    })
}

pub fn register(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(get_function_from_registry, parent)?)?;

    Ok(())
}
