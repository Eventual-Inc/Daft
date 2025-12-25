use std::{fmt::Display, num::NonZeroUsize, sync::Arc};

use common_error::DaftResult;
use common_metrics::MetricsCollector;
#[cfg(feature = "python")]
use common_metrics::python::PyOperatorMetrics;
use daft_core::{prelude::*, series::Series};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
use crate::python_udf::{
    collect_operator_metrics,
    retry::{retry_with_backoff, retry_with_backoff_async},
};
use crate::{
    Expr, ExprRef,
    functions::{
        python::{OnError, RuntimePyObject},
        scalar::ScalarFn,
    },
    python_udf::PyScalarFn,
};

#[allow(clippy::too_many_arguments)]
pub fn batch_udf(
    name: &str,
    cls: RuntimePyObject,
    method: RuntimePyObject,
    is_async: bool,
    return_dtype: DataType,
    gpus: usize,
    use_process: Option<bool>,
    max_concurrency: Option<NonZeroUsize>,
    batch_size: Option<usize>,
    original_args: RuntimePyObject,
    args: Vec<ExprRef>,
    max_retries: Option<usize>,
    on_error: OnError,
) -> Expr {
    Expr::ScalarFn(ScalarFn::Python(PyScalarFn::Batch(BatchPyFn {
        function_name: Arc::from(name),
        cls,
        method,
        is_async,
        return_dtype,
        gpus,
        use_process,
        max_concurrency,
        batch_size,
        original_args,
        args,
        max_retries,
        on_error,
    })))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BatchPyFn {
    pub function_name: Arc<str>,
    pub cls: RuntimePyObject,
    pub method: RuntimePyObject,
    pub is_async: bool,
    pub return_dtype: DataType,
    pub gpus: usize,
    pub use_process: Option<bool>,
    pub max_concurrency: Option<NonZeroUsize>,
    pub batch_size: Option<usize>,
    pub original_args: RuntimePyObject,
    pub args: Vec<ExprRef>,
    pub max_retries: Option<usize>,
    pub on_error: OnError,
}

impl Display for BatchPyFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let children_str = self.args.iter().map(|expr| expr.to_string()).join(", ");

        write!(f, "{}({})", self.function_name, children_str)
    }
}

impl BatchPyFn {
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
            gpus: self.gpus,
            use_process: self.use_process,
            max_concurrency: self.max_concurrency,
            batch_size: self.batch_size,
            original_args: self.original_args.clone(),
            args: children,
            max_retries: self.max_retries,
            on_error: self.on_error,
        }
    }

    #[cfg(feature = "python")]
    pub fn call(
        &self,
        args: &[Series],
        metrics_collector: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;
        let max_retries = self.max_retries.unwrap_or(0);
        let py_args = args
            .iter()
            .map(|s| PySeries::from(s.clone()))
            .collect::<Vec<_>>();

        let try_call_batch = || {
            Python::attach(|py| {
                let func = py
                    .import(pyo3::intern!(py, "daft.udf.execution"))?
                    .getattr(pyo3::intern!(py, "call_batch_func"))?;

                let result = func.call1((
                    self.cls.as_ref(),
                    self.method.as_ref(),
                    self.original_args.as_ref(),
                    py_args.clone(),
                ))?;

                let (result_series, operator_metrics): (PySeries, PyOperatorMetrics) =
                    result.extract()?;

                DaftResult::Ok((result_series.series, operator_metrics))
            })
        };

        let result = retry_with_backoff(None, try_call_batch, max_retries);
        let name = args[0].name();

        if let Ok((_, operator_metrics)) = &result {
            collect_operator_metrics(operator_metrics, metrics_collector);
        }

        match (result, self.on_error) {
            (Ok((result_series, _)), _) => Ok(result_series.cast(&self.return_dtype)?.rename(name)),
            (Err(err), OnError::Raise) => Err(err),
            (Err(err), OnError::Log) => {
                log::warn!("Python UDF error: {}", err);
                // todo: log error
                let num_rows = args.iter().map(Series::len).max().unwrap();

                // log::error!("Python UDF error: {}", err);
                Ok(Series::full_null(name, &self.return_dtype, num_rows))
            }
            (Err(_), OnError::Ignore) => {
                let num_rows = args.iter().map(Series::len).max().unwrap();
                Ok(Series::full_null(name, &self.return_dtype, num_rows))
            }
        }
    }

    #[cfg(not(feature = "python"))]
    pub async fn call_async(
        &self,
        _args: &[Series],
        _metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        panic!("Cannot evaluate a BatchPyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub async fn call_async(
        &self,
        args: &[Series],
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        let max_retries = self.max_retries.unwrap_or(0);
        let name = args[0].name();

        let result = retry_with_backoff_async(
            || async { self.call_async_once(args, name).await },
            max_retries,
        )
        .await;

        if let Ok((_, operator_metrics)) = &result {
            collect_operator_metrics(operator_metrics, metrics);
        }

        match (result, self.on_error) {
            (Ok((result_series, _)), _) => Ok(result_series.cast(&self.return_dtype)?.rename(name)),
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
    async fn call_async_once(
        &self,
        args: &[Series],
        name: &str,
    ) -> DaftResult<(Series, common_metrics::python::PyOperatorMetrics)> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        let cls = self.cls.clone();
        let method = self.method.clone();
        let original_args = self.original_args.clone();
        let args = args.to_vec();

        let (py_series, operator_metrics) = common_runtime::python::execute_python_coroutine::<
            _,
            (PySeries, PyOperatorMetrics),
        >(move |py| {
            let f = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_batch_async"))?;

            let evaluated_args = args
                .iter()
                .map(|s| PySeries::from(s.clone()))
                .collect::<Vec<_>>();

            let coroutine = f.call1((
                cls.as_ref(),
                method.as_ref(),
                original_args.as_ref(),
                evaluated_args,
            ))?;

            Ok(coroutine)
        })
        .await?;

        Ok((py_series.series.rename(name), operator_metrics))
    }

    #[cfg(not(feature = "python"))]
    pub fn call(
        &self,
        _args: &[Series],
        _metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        panic!("Cannot evaluate a BatchPyFn without compiling for Python");
    }
}
