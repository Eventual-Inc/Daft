use std::{collections::HashMap, fmt::Display, num::NonZeroUsize, sync::Arc};

use common_error::DaftResult;
use daft_core::{prelude::*, series::Series};
use itertools::Itertools;
use opentelemetry::{
    Key,
    logs::{AnyValue, LogRecord, Logger, LoggerProvider},
};
#[cfg(feature = "python")]
use pyo3::{PyErr, Python, prelude::*};
use serde::{Deserialize, Serialize};

use crate::{
    Expr, ExprRef,
    functions::{python::RuntimePyObject, scalar::ScalarFn},
    operator_metrics::MetricsCollector,
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
    pub fn call(
        &self,
        _args: &[Series],
        _metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub fn call(&self, args: &[Series], metrics: &mut dyn MetricsCollector) -> DaftResult<Series> {
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
        self.call_serial(args, num_rows, metrics)
    }

    #[cfg(not(feature = "python"))]
    pub async fn call_async(
        &self,
        _args: &[Series],
        _metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub async fn call_async(
        &self,
        args: &[Series],
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
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

        let mut result_series = self
            .call_async_batch_once(args, num_rows, name, metrics)
            .await;

        for _attempt in 0..max_retries {
            if result_series.is_ok() {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);

            result_series = self
                .call_async_batch_once(args, num_rows, name, metrics)
                .await;
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

                let logger_provider = common_tracing::GLOBAL_LOGGER_PROVIDER.lock().unwrap();
                if let Some(logger_provider) = logger_provider.as_ref() {
                    let logger = logger_provider.logger("python-udf-error");
                    let mut log_record = logger.create_log_record();

                    if let DaftError::PyO3Error(py_err) = &err {
                        Python::attach(|py| {
                            Self::capture_exception_details(py, py_err, &mut log_record);
                        });
                    } else {
                        log_record.set_body(format!("{err}").into());
                    }

                    logger.emit(log_record);
                }

                Ok(Series::full_null(name, &self.return_dtype, num_rows))
            }
            (Err(_), OnError::Ignore) => {
                let num_rows = args.iter().map(Series::len).max().unwrap();
                Ok(Series::full_null(name, &self.return_dtype, num_rows))
            }
        }
    }

    /// Serializes a Python object to a string for logging purposes.
    ///
    /// Rules:
    /// - Fixed-size types (ints, floats, bools) → string representation
    /// - Strings → whole string value
    /// - Complex/arbitrary-size types (lists, dicts, numpy arrays, etc.) → placeholder string
    #[cfg(feature = "python")]
    fn serialize_pyobject_for_logging(obj: &pyo3::Bound<PyAny>) -> String {
        // Use Python's str() to stringify the object
        let str_result = obj
            .str()
            .and_then(|value| value.to_str().map(|s| s.to_string()));

        match str_result {
            Ok(val) => {
                // Limit string parameters to first 100 characters
                if val.len() > 100 {
                    let truncated: String = val.chars().take(100).collect();
                    format!("{}...", truncated)
                } else {
                    val
                }
            }
            Err(_) => "<unable_to_stringify>".to_string(),
        }
    }

    #[cfg(feature = "python")]
    pub(crate) fn capture_exception_details<R: LogRecord>(
        py: Python,
        py_err: &PyErr,
        log_record: &mut R,
    ) {
        let exception_type = py_err.get_type(py);
        let exception_value = py_err.value(py);

        let type_str = exception_type
            .name()
            .and_then(|name| name.to_str().map(|s| s.to_string()));

        let msg_str = exception_value
            .str()
            .and_then(|value| value.to_str().map(|s| s.to_string()));

        if let Ok(msg_str) = msg_str {
            log_record.set_body(AnyValue::String(msg_str.into()));
        }

        if let Ok(type_str) = type_str {
            log_record.add_attribute("type", AnyValue::String(type_str.into()));
        }

        let format_exception = py
            .import(pyo3::intern!(py, "traceback"))
            .and_then(|module| module.getattr("format_exception"));

        if let Ok(format_exception) = format_exception {
            let exception_traceback = py_err.traceback(py);

            let formatted =
                format_exception.call1((exception_type, exception_value, exception_traceback));

            if let Ok(formatted) = &formatted {
                let formatted_list = formatted.cast::<pyo3::types::PyList>();

                if let Ok(formatted_list) = formatted_list {
                    let formatted_list: Vec<AnyValue> = formatted_list
                        .iter()
                        .map(|frame| {
                            frame
                                .extract::<String>()
                                .unwrap_or_else(|_| "<unrepresentable frame>".to_string())
                        })
                        .map(|frame| frame.into())
                        .collect();

                    log_record
                        .add_attribute("traceback", AnyValue::ListAny(Box::new(formatted_list)));
                }
            }
        }
    }

    #[cfg(feature = "python")]
    fn call_serial(
        &self,
        args: &[Series],
        num_rows: usize,
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
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

        fn retry<F: FnMut() -> DaftResult<Literal>>(
            py: Python,
            mut func: F,
            max_retries: usize,
            mut delay_ms: u64,
        ) -> DaftResult<Literal> {
            let mut retries = 0;
            loop {
                match func() {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if retries >= max_retries {
                            return Err(e);
                        }

                        // Update our failure map for next iteration
                        use std::{thread, time::Duration};
                        py.detach(|| {
                            thread::sleep(Duration::from_millis(delay_ms));
                        });
                        // Exponential backoff: multiply by 2, cap at MAX_DELAY_MS
                        delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                    }
                }
                retries += 1;
            }
        }

        let s = Python::attach(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_func"))?;

            // Collect argument names (same for all rows)
            let arg_names: Vec<String> = args.iter().map(|s| s.name().to_string()).collect();

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
                        .and_then(|res| {
                            use common_metrics::python::PyOperatorMetrics;

                            let (value_obj, operator_metrics): (Bound<PyAny>, PyOperatorMetrics) =
                                res.extract()?;
                            let literal =
                                Literal::from_pyobj(&value_obj, Some(&self.return_dtype))?;
                            for (key, counter) in operator_metrics.inner {
                                metrics.inc_counter(
                                    &key,
                                    counter.value,
                                    counter.description.as_deref(),
                                    Some(counter.attributes),
                                );
                            }
                            Ok(literal)
                        })
                        .map_err(DaftError::from)
                };
                let res = retry(py, f, max_retries, delay_ms);

                // Handle on_error logic outside of retry
                let final_res = match res {
                    Ok(result) => Ok(result),
                    Err(e) => match on_error {
                        OnError::Raise => Err(e),
                        OnError::Log => {
                            let lg = common_tracing::GLOBAL_LOGGER_PROVIDER.lock().unwrap();
                            if let Some(logger_provider) = lg.as_ref() {
                                let logger = logger_provider.logger("python-udf-error");
                                let mut log_record = logger.create_log_record();

                                if let DaftError::PyO3Error(py_err) = &e {
                                    Self::capture_exception_details(py, py_err, &mut log_record);
                                } else {
                                    log_record.set_body(format!("{e}").into());
                                }

                                // Add UDF argument names and values to the log record as a Map
                                // Limit to first 20 args to avoid bloating logs
                                let mut udf_args_map: HashMap<Key, AnyValue> = HashMap::new();
                                let total_args = arg_names.len().min(py_args.len());
                                for (arg_name, py_arg) in
                                    arg_names.iter().zip(py_args.iter()).take(20)
                                {
                                    let serialized_value =
                                        Self::serialize_pyobject_for_logging(py_arg);
                                    udf_args_map.insert(
                                        Key::new(arg_name.clone()),
                                        AnyValue::String(serialized_value.into()),
                                    );
                                }
                                // Indicate if additional arguments were truncated
                                if total_args > 20 {
                                    udf_args_map.insert(
                                        Key::new("<TRUNCATED_ARGS>".to_string()),
                                        AnyValue::String(
                                            format!(
                                                "{} additional arguments truncated",
                                                total_args - 20
                                            )
                                            .into(),
                                        ),
                                    );
                                }
                                log_record.add_attribute(
                                    "udf_args",
                                    AnyValue::Map(Box::new(udf_args_map)),
                                );

                                logger.emit(log_record);
                            }

                            log::warn!("Python UDF error: {}", e);
                            Ok(Literal::Null)
                        }
                        OnError::Ignore => Ok(Literal::Null),
                    },
                };

                py_args.clear();
                final_res
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
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        use common_metrics::python::PyOperatorMetrics;
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        let cls = self.cls.clone();
        let method = self.method.clone();
        let original_args = self.original_args.clone();
        let args = args.to_vec();
        let return_dtype = self.return_dtype.clone();

        let (py_series, operator_metrics) = common_runtime::python::execute_python_coroutine::<
            _,
            (PySeries, PyOperatorMetrics),
        >(move |py| {
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
            let py_return_type = daft_core::python::PyDataType::from(return_dtype.clone());
            let coroutine = f.call1((
                cls.as_ref(),
                method.as_ref(),
                py_return_type,
                original_args.as_ref(),
                evaluated_args,
            ))?;
            Ok(coroutine)
        })
        .await?;
        for (key, counter) in operator_metrics.inner {
            metrics.inc_counter(
                &key,
                counter.value,
                counter.description.as_deref(),
                Some(counter.attributes),
            );
        }

        Ok(py_series.series.rename(name))
    }
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use std::ffi::CString;

    use opentelemetry::logs::{AnyValue, Logger, LoggerProvider};
    use opentelemetry_sdk::{Resource, logs::SdkLoggerProvider};
    use pyo3::prelude::*;

    use crate::python_udf::RowWisePyFn;

    #[test]
    fn capture_exception_details_includes_type_and_traceback() {
        Python::initialize();
        Python::attach(|py| {
            let code = r#"
def failing():
    raise RuntimeError("boom")
"#;
            let module = PyModule::new(py, "test_mod").unwrap();
            let code_cstr = CString::new(code).unwrap();
            py.run(code_cstr.as_c_str(), Some(&module.dict()), None)
                .unwrap();
            let func = module.getattr("failing").unwrap();

            let result = func.call0();
            assert!(result.is_err());
            let err = result.unwrap_err();

            // Create an in-memory logger provider for testing
            let logger_provider = SdkLoggerProvider::builder()
                .with_resource(Resource::builder().build())
                .build();
            let logger = logger_provider.logger("test-logger");
            let mut log_record = logger.create_log_record();

            RowWisePyFn::capture_exception_details(py, &err, &mut log_record);

            // Verify the log record has a body with the error message
            let body = log_record.body();
            assert!(body.is_some(), "Log record should have a body");
            if let Some(AnyValue::String(body_str)) = body {
                assert!(
                    body_str.as_str().contains("boom"),
                    "Body should contain error message"
                );
            }

            // Verify the log record has the "type" attribute
            let type_attr = log_record.attributes_iter().find(|(key, value)| {
                key.as_str() == "type"
                    && if let AnyValue::String(s) = value {
                        s.as_str() == "RuntimeError"
                    } else {
                        false
                    }
            });
            assert!(
                type_attr.is_some(),
                "Log record should have a 'type' attribute with value 'RuntimeError'"
            );

            // Verify the log record has the "traceback" attribute
            let traceback_attr = log_record
                .attributes_iter()
                .find(|(key, _)| key.as_str() == "traceback");
            assert!(
                traceback_attr.is_some(),
                "Log record should have a 'traceback' attribute"
            );

            // Verify traceback contains the function name "failing"
            if let Some((_, AnyValue::ListAny(traceback_list))) = traceback_attr {
                assert!(!traceback_list.is_empty(), "Traceback should not be empty");
                let traceback_str: String = traceback_list
                    .iter()
                    .map(|v| {
                        if let AnyValue::String(s) = v {
                            s.as_str().to_string()
                        } else {
                            String::new()
                        }
                    })
                    .collect();
                assert!(
                    traceback_str.contains("failing"),
                    "Traceback should contain function name 'failing'"
                );
            }
        });
    }
}
