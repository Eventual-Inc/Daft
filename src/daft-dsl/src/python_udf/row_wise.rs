use std::{fmt::Display, num::NonZeroUsize, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    Expr, ExprRef,
    functions::{python::RuntimePyObject, scalar::ScalarFn},
    python_udf::PyScalarFn,
};

#[allow(clippy::too_many_arguments)]
pub fn row_wise_udf(
    name: &str,
    cls: RuntimePyObject,
    method: RuntimePyObject,
    is_async: bool,
    return_dtype: DataType,
    gpus: usize,
    use_process: Option<bool>,
    max_concurrency: Option<NonZeroUsize>,
    max_retries: Option<usize>,
    on_error: crate::functions::python::OnError,
    original_args: RuntimePyObject,
    args: Vec<ExprRef>,
) -> Expr {
    Expr::ScalarFn(ScalarFn::Python(PyScalarFn::RowWise(RowWisePyFn {
        function_name: Arc::from(name),
        cls,
        method,
        is_async,
        return_dtype,
        original_args,
        on_error,
        max_retries,
        args,
        gpus,
        use_process,
        max_concurrency,
    })))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowWisePyFn {
    pub function_name: Arc<str>,
    pub cls: RuntimePyObject,
    pub method: RuntimePyObject,
    pub is_async: bool,
    pub return_dtype: DataType,
    pub original_args: RuntimePyObject,
    pub args: Vec<ExprRef>,
    pub gpus: usize,
    pub use_process: Option<bool>,
    pub max_concurrency: Option<NonZeroUsize>,
    pub max_retries: Option<usize>,
    pub on_error: crate::functions::python::OnError,
}

impl Display for RowWisePyFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let children_str = self.args.iter().map(|expr| expr.to_string()).join(", ");

        write!(f, "{}({})", self.function_name, children_str)
    }
}

impl RowWisePyFn {
    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        assert_eq!(
            children.len(),
            self.args.len(),
            "There must be the same amount of new children as original."
        );

        Self {
            function_name: self.function_name.clone(),
            cls: self.cls.clone(),
            method: self.method.clone(),
            is_async: self.is_async,
            return_dtype: self.return_dtype.clone(),
            original_args: self.original_args.clone(),
            args: children,
            gpus: self.gpus,
            use_process: self.use_process,
            max_concurrency: self.max_concurrency,
            max_retries: self.max_retries,
            on_error: self.on_error,
        }
    }

    #[cfg(not(feature = "python"))]
    pub fn call(&self, _args: &[Series]) -> DaftResult<Series> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub fn call(&self, args: &[Series]) -> DaftResult<Series> {
        let num_rows = args
            .iter()
            .map(Series::len)
            .max()
            .expect("RowWisePyFn should have at least one argument");

        for a in args {
            assert!(
                a.len() == num_rows || a.len() == 1,
                "arg lengths differ: {} vs {}",
                num_rows,
                a.len()
            );
        }
        self.call_serial(args, num_rows)
    }

    #[cfg(not(feature = "python"))]
    pub async fn call_async(&self, _args: &[Series]) -> DaftResult<Series> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub async fn call_async(&self, args: &[Series]) -> DaftResult<Series> {
        use common_error::DaftError;

        use crate::functions::python::OnError;
        let num_rows = args
            .iter()
            .map(Series::len)
            .max()
            .expect("RowWisePyFn should have at least one argument");

        for a in args {
            assert!(
                a.len() == num_rows || a.len() == 1,
                "arg lengths differ: {} vs {}",
                num_rows,
                a.len()
            );
        }

        let max_retries = self.max_retries.unwrap_or(0);

        let name = args[0].name();

        // TODO(cory): consider exposing delay and max_delay to users.
        let mut delay_ms: u64 = 100; // Start with 100 ms
        const MAX_DELAY_MS: u64 = 60000; // Max 60 seconds

        let mut result_series = self.call_async_batch_once(args, num_rows, name).await;

        for _attempt in 0..max_retries {
            if result_series.is_ok() {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);

            result_series = self.call_async_batch_once(args, num_rows, name).await;
        }
        let name = args[0].name();

        let result_series = result_series
            .map_err(DaftError::from)
            .and_then(|s| Ok(s.cast(&self.return_dtype)?.rename(name)));

        match (result_series, self.on_error) {
            (Ok(result_series), _) => Ok(result_series),
            (Err(err), OnError::Raise) => Err(err),
            (Err(err), OnError::Log) => {
                log::warn!("Python UDF error: {}", err);
                let num_rows = args.iter().map(Series::len).max().unwrap();

                Ok(Series::full_null(name, &self.return_dtype, num_rows))
            }
            (Err(_), OnError::Ignore) => {
                let num_rows = args.iter().map(Series::len).max().unwrap();
                Ok(Series::full_null(name, &self.return_dtype, num_rows))
            }
        }
    }

    #[cfg(feature = "python")]
    fn call_serial(&self, args: &[Series], num_rows: usize) -> DaftResult<Series> {
        use common_error::DaftError;
        use daft_core::series::from_lit::series_from_literals_iter;
        use pyo3::prelude::*;

        use crate::functions::python::OnError;

        let cls_ref = self.cls.as_ref();
        let method_ref = self.method.as_ref();
        let args_ref = self.original_args.as_ref();

        let name = args[0].name();
        let on_error = self.on_error;
        let max_retries = self.max_retries.unwrap_or(0);
        let delay_ms: u64 = 100; // Start with 100 ms
        const MAX_DELAY_MS: u64 = 60000; // Max 60 seconds

        fn retry<F: Fn() -> DaftResult<Literal>>(
            py: Python,
            func: F,
            max_retries: usize,
            on_error: OnError,
            mut delay_ms: u64,
        ) -> DaftResult<Literal> {
            let mut res = Ok(Literal::Null);

            for attempt in 0..=max_retries {
                match func() {
                    Ok(result) => {
                        res = Ok(result);
                        break;
                    }
                    Err(e) => {
                        if attempt >= max_retries {
                            match on_error {
                                OnError::Raise => res = Err(e),
                                OnError::Log => {
                                    log::warn!("Retrying function call after error: {}", e);
                                    res = Ok(Literal::Null);
                                }
                                OnError::Ignore => {
                                    res = Ok(Literal::Null);
                                }
                            }
                            break;
                        }
                        // Update our failure map for next iteration
                        if attempt < max_retries {
                            use std::{thread, time::Duration};
                            py.detach(|| {
                                thread::sleep(Duration::from_millis(delay_ms));
                            });
                            // Exponential backoff: multiply by 2, cap at MAX_DELAY_MS
                            delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                        }
                    }
                }
            }
            res
        }
        let s = Python::attach(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_func"))?;

            // pre-allocating py_args vector so we're not creating a new vector for each iteration
            let mut py_args = Vec::with_capacity(args.len());

            let iter = (0..num_rows).map(|i| {
                for s in args {
                    let idx = if s.len() == 1 { 0 } else { i };
                    let lit = s.get_lit(idx);
                    let pyarg = lit.into_pyobject(py)?;
                    py_args.push(pyarg);
                }

                let f = || {
                    func.call1((cls_ref, method_ref, args_ref, &py_args))
                        .and_then(|res| Literal::from_pyobj(&res, Some(&self.return_dtype)))
                        .map_err(DaftError::from)
                };
                let res = retry(py, f, max_retries, on_error, delay_ms);
                py_args.clear();
                res
            });
            series_from_literals_iter(iter, Some(self.return_dtype.clone()))
        })?
        .rename(name);

        Ok(s)
    }

    #[cfg(feature = "python")]
    async fn call_async_batch_once(
        &self,
        args: &[Series],
        num_rows: usize,
        name: &str,
    ) -> DaftResult<Series> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        let cls = self.cls.clone();
        let method = self.method.clone();
        let original_args = self.original_args.clone();
        let args = args.to_vec();
        let py_return_type = daft_core::python::PyDataType::from(self.return_dtype.clone());

        common_runtime::python::execute_python_coroutine::<_, PySeries>(move |py| {
            let f = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_async_func_batched"))?;
            let mut evaluated_args = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let py_args_for_row = args
                    .iter()
                    .map(|s| {
                        let idx = if s.len() == 1 { 0 } else { i };
                        let lit = s.get_lit(idx);
                        lit.into_pyobject(py)
                            .map_err(|e| e.into())
                            .map(|obj| obj.unbind())
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                evaluated_args.push(py_args_for_row);
            }
            f.call1((
                cls.as_ref(),
                method.as_ref(),
                py_return_type,
                original_args.as_ref(),
                evaluated_args,
            ))
        })
        .await
        .map(|py_series| py_series.series.rename(name))
    }
}
