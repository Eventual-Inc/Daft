mod runtime_py_object;

use std::{hash::Hash, num::NonZeroUsize, str::FromStr, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_treenode::{TreeNode, TreeNodeRecursion};
#[cfg(feature = "python")]
use pyo3::{Bound, Py, PyAny, PyResult, Python, call::PyCallArgs, prelude::*, types::PyDict};
pub use runtime_py_object::RuntimePyObject;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum OnError {
    #[default]
    Raise,
    Log,
    Ignore,
}
impl std::fmt::Display for OnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raise => write!(f, "raise"),
            Self::Log => write!(f, "log"),
            Self::Ignore => write!(f, "ignore"),
        }
    }
}

impl OnError {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Raise => "raise",
            Self::Log => "log",
            Self::Ignore => "ignore",
        }
    }
}

impl FromStr for OnError {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "raise" => Ok(Self::Raise),
            "log" => Ok(Self::Log),
            "ignore" => Ok(Self::Ignore),
            _ => Err(DaftError::ValueError(format!(
                "Invalid on_error value: {}",
                s
            ))),
        }
    }
}

#[cfg(feature = "python")]
use crate::python::PyExpr;
use crate::{Expr, ExprRef, functions::scalar::ScalarFn, python_udf::PyScalarFn};

/// rust wrapper around `daft.udf.py:UDF`
#[derive(Debug, Clone)]
pub struct WrappedUDFClass {
    #[cfg(feature = "python")]
    pub inner: Arc<Py<PyAny>>,
}

#[cfg(feature = "python")]
impl WrappedUDFClass {
    pub fn call<'py, A>(
        &self,
        py: Python<'py>,
        args: A,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<PyExpr>
    where
        A: PyCallArgs<'py>,
    {
        let o = self.inner.call(py, args, kwargs)?;
        let inner = o.getattr(py, "_expr")?;

        let expr = inner.extract::<PyExpr>(py)?;
        Ok(expr)
    }

    pub fn name(&self) -> PyResult<String> {
        Python::attach(|py| {
            let s: String = self.inner.getattr(py, "name")?.extract(py)?;
            Ok(if s.contains('.') {
                s.split('.').next_back().unwrap().to_string()
            } else {
                s
            })
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UDFProperties {
    pub name: String,
    pub resource_request: Option<ResourceRequest>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<NonZeroUsize>,
    pub use_process: Option<bool>,
    pub max_retries: Option<usize>,
    pub builtin_name: bool,
    pub is_async: bool,
    pub is_scalar: bool,
    pub on_error: Option<OnError>,
    pub ray_options: Option<RuntimePyObject>,
}

impl UDFProperties {
    pub fn from_expr(expr: &ExprRef) -> DaftResult<Self> {
        let mut udf_properties = None;
        let mut num_udfs = 0;

        expr.apply(|e| {
            match e.as_ref() {
                Expr::ScalarFn(ScalarFn::Python(PyScalarFn::RowWise(row_wise_fn))) => {
                    num_udfs += 1;
                    let rr = ResourceRequest::try_new_internal(
                        row_wise_fn.cpus.as_ref().map(|v| v.0),
                        Some(row_wise_fn.gpus.0),
                        None,
                    )?;

                    #[cfg(feature = "python")]
                    let rr = Python::attach(|py| {
                        let mut rr = rr;
                        if let Some(options) = row_wise_fn
                            .ray_options
                            .as_ref()
                            .and_then(|o| o.as_ref().bind(py).cast::<PyDict>().ok())
                            && let Some(memory) = options
                                .get_item("memory")
                                .ok()
                                .flatten()
                                .and_then(|v| v.extract::<usize>().ok())
                        {
                            rr = ResourceRequest::try_new_internal(
                                rr.num_cpus(),
                                rr.num_gpus(),
                                Some(memory),
                            )
                            .unwrap_or(rr);
                        }
                        rr
                    });

                    udf_properties = Some(Self {
                        name: row_wise_fn.function_name.to_string(),
                        resource_request: Some(rr),
                        batch_size: None,
                        concurrency: row_wise_fn.max_concurrency,
                        use_process: row_wise_fn.use_process,
                        max_retries: row_wise_fn.max_retries,
                        builtin_name: row_wise_fn.builtin_name,
                        is_async: row_wise_fn.is_async,
                        on_error: Some(row_wise_fn.on_error),
                        is_scalar: false,
                        ray_options: row_wise_fn.ray_options.clone(),
                    });
                }
                Expr::ScalarFn(ScalarFn::Python(PyScalarFn::Batch(batch_fn))) => {
                    num_udfs += 1;
                    let rr = ResourceRequest::try_new_internal(
                        batch_fn.cpus.as_ref().map(|v| v.0),
                        Some(batch_fn.gpus.0),
                        None,
                    )?;

                    #[cfg(feature = "python")]
                    let rr = Python::attach(|py| {
                        let mut rr = rr;
                        if let Some(options) = batch_fn
                            .ray_options
                            .as_ref()
                            .and_then(|o| o.as_ref().bind(py).cast::<PyDict>().ok())
                            && let Some(memory) = options
                                .get_item("memory")
                                .ok()
                                .flatten()
                                .and_then(|v| v.extract::<usize>().ok())
                        {
                            rr = ResourceRequest::try_new_internal(
                                rr.num_cpus(),
                                rr.num_gpus(),
                                Some(memory),
                            )
                            .unwrap_or(rr);
                        }
                        rr
                    });

                    udf_properties = Some(Self {
                        name: batch_fn.function_name.to_string(),
                        resource_request: Some(rr),
                        batch_size: batch_fn.batch_size,
                        concurrency: batch_fn.max_concurrency,
                        use_process: batch_fn.use_process,
                        max_retries: batch_fn.max_retries,
                        is_async: batch_fn.is_async,
                        on_error: Some(batch_fn.on_error),
                        builtin_name: batch_fn.builtin_name,
                        is_scalar: false,
                        ray_options: batch_fn.ray_options.clone(),
                    });
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();

        if num_udfs != 1 {
            Err(DaftError::ValueError(format!(
                "Expected exactly one UDF in expression, got {} UDFs",
                num_udfs
            )))
        } else {
            Ok(udf_properties.expect("Expect a UDF to be found"))
        }
    }

    pub fn is_actor_pool_udf(&self) -> bool {
        self.concurrency.is_some() && !self.is_async
    }

    #[must_use]
    pub fn multiline_display(&self, include_resource_properties: bool) -> Vec<String> {
        let mut properties = vec![];

        if include_resource_properties && let Some(resource_request) = &self.resource_request {
            properties.extend(resource_request.multiline_display());
        }

        if let Some(batch_size) = &self.batch_size {
            properties.push(format!("batch_size = {}", batch_size));
        }

        if let Some(concurrency) = &self.concurrency {
            properties.push(format!("concurrency = {}", concurrency));
        }

        if let Some(use_process) = &self.use_process {
            properties.push(format!("use_process = {}", use_process));
        }

        if let Some(max_retries) = &self.max_retries {
            properties.push(format!("max_retries = {}", max_retries));
        }

        if let Some(on_error) = &self.on_error {
            properties.push(format!("on_error = {}", on_error));
        }

        properties.push(format!("async = {}", &self.is_async));
        properties.push(format!("scalar = {}", &self.is_scalar));

        #[cfg(feature = "python")]
        {
            if let Some(ray_options) = &self.ray_options {
                // FIXME(zhenchao) Perhaps the layout should be optimized to improve readability
                let ray_options = Python::attach(|py| -> PyResult<Option<String>> {
                    let bound = ray_options.as_ref().bind(py);
                    if let Ok(dict) = bound.cast::<PyDict>() {
                        if dict.is_empty() {
                            return Ok(None);
                        }
                        let repr = dict.repr()?;
                        Ok(Some(format!("ray_options = {}", repr)))
                    } else {
                        Ok(None)
                    }
                })
                .ok()
                .flatten();

                if let Some(prop) = ray_options {
                    properties.push(prop);
                }
            }
        }

        properties
    }
}
