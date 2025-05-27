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

mod binary;
mod coalesce;
mod distance;
mod misc;
mod sequence;
mod temporal;
mod tokenize;

use std::sync::Arc;

use daft_dsl::{
    functions::{FunctionArg, FunctionArgs, ScalarFunction, ScalarUDF, FUNCTION_REGISTRY},
    python::PyExpr,
    ExprRef,
};
use pyo3::{
    types::{PyDict, PyModule, PyModuleMethods, PyTuple},
    wrap_pyfunction, Bound, PyResult,
};

#[pyo3::pyclass]
pub struct PyScalarFunction {
    pub inner: Arc<dyn ScalarUDF>,
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

        let expr: ExprRef = ScalarFunction {
            udf: self.inner.clone(),
            inputs: FunctionArgs::try_new(inputs)?,
        }
        .into();

        Ok(expr.into())
    }
}

#[pyo3::pyfunction]
pub fn get_function_from_registry(name: &str) -> PyResult<PyScalarFunction> {
    let f = FUNCTION_REGISTRY.read().unwrap().get(name);
    if let Some(f) = f {
        Ok(PyScalarFunction { inner: f })
    } else {
        panic!("Function not found in registry: {}", name)
    }
}

pub fn register(parent: &Bound<PyModule>) -> PyResult<()> {
    macro_rules! add {
        ($p:path) => {
            parent.add_function(wrap_pyfunction!($p, parent)?)?;
        };
    }
    parent.add_function(wrap_pyfunction!(get_function_from_registry, parent)?)?;

    add!(coalesce::coalesce);
    add!(distance::cosine_distance);
    add!(binary::binary_length);
    add!(binary::binary_concat);
    add!(binary::binary_slice);

    add!(binary::encode);
    add!(binary::decode);
    add!(binary::try_encode);
    add!(binary::try_decode);

    add!(misc::to_struct);
    add!(misc::hash);
    add!(misc::minhash);

    add!(sequence::monotonically_increasing_id);

    add!(temporal::dt_date);
    add!(temporal::dt_day);
    add!(temporal::dt_day_of_week);
    add!(temporal::dt_day_of_month);
    add!(temporal::dt_day_of_year);
    add!(temporal::dt_week_of_year);
    add!(temporal::dt_hour);
    add!(temporal::dt_minute);
    add!(temporal::dt_month);
    add!(temporal::dt_quarter);
    add!(temporal::dt_second);
    add!(temporal::dt_millisecond);
    add!(temporal::dt_microsecond);
    add!(temporal::dt_nanosecond);
    add!(temporal::dt_unix_date);
    add!(temporal::dt_time);
    add!(temporal::dt_year);
    add!(temporal::dt_truncate);
    add!(temporal::dt_to_unix_epoch);
    add!(temporal::dt_strftime);

    add!(tokenize::tokenize_encode);
    add!(tokenize::tokenize_decode);

    Ok(())
}
