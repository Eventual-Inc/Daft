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
mod list;
mod misc;
mod sequence;
mod temporal;
mod tokenize;
mod uri;
mod utf8;

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

    add!(list::list_chunk);
    add!(list::list_count);
    add!(list::explode);
    add!(list::list_get);
    add!(list::list_join);
    add!(list::list_max);
    add!(list::list_mean);
    add!(list::list_min);
    add!(list::list_slice);
    add!(list::list_sort);
    add!(list::list_sum);
    add!(list::list_count_distinct);
    add!(list::list_value_counts);
    add!(list::list_distinct);
    add!(list::list_bool_and);
    add!(list::list_bool_or);

    add!(misc::to_struct);
    add!(misc::utf8_count_matches);
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

    add!(uri::url_download);
    add!(uri::url_upload);

    add!(utf8::utf8_capitalize);
    add!(utf8::utf8_contains);
    add!(utf8::utf8_endswith);
    add!(utf8::utf8_extract);
    add!(utf8::utf8_extract_all);
    add!(utf8::utf8_find);
    add!(utf8::utf8_ilike);
    add!(utf8::utf8_left);
    add!(utf8::utf8_length);
    add!(utf8::utf8_length_bytes);
    add!(utf8::utf8_like);
    add!(utf8::utf8_lower);
    add!(utf8::utf8_lpad);
    add!(utf8::utf8_lstrip);
    add!(utf8::utf8_match);
    add!(utf8::utf8_repeat);
    add!(utf8::utf8_replace);
    add!(utf8::utf8_reverse);
    add!(utf8::utf8_right);
    add!(utf8::utf8_rpad);
    add!(utf8::utf8_rstrip);
    add!(utf8::utf8_split);
    add!(utf8::utf8_startswith);
    add!(utf8::utf8_substr);
    add!(utf8::utf8_upper);
    add!(utf8::utf8_normalize);
    add!(utf8::utf8_to_date);
    add!(utf8::utf8_to_datetime);

    Ok(())
}
