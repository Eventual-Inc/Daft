mod runtime_py_object;
mod udf;

use std::{hash::Hash, num::NonZeroUsize, str::FromStr, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_core::prelude::*;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::{Bound, Py, PyAny, PyResult, Python, call::PyCallArgs, types::PyDict};
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

use super::FunctionExpr;
#[cfg(feature = "python")]
use crate::python::PyExpr;
use crate::{
    Expr, ExprRef,
    functions::scalar::ScalarFn,
    python_udf::{BatchPyFn, PyScalarFn},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MaybeInitializedUDF {
    Initialized(RuntimePyObject),
    Uninitialized {
        inner: RuntimePyObject,
        init_args: RuntimePyObject,
    },
}

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
pub struct LegacyPythonUDF {
    pub name: Arc<String>,
    pub func: MaybeInitializedUDF,
    pub bound_args: RuntimePyObject,
    pub num_expressions: usize,
    pub return_dtype: DataType,
    pub resource_request: Option<ResourceRequest>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<NonZeroUsize>,
    pub use_process: Option<bool>,
    pub ray_options: Option<RuntimePyObject>,
}

impl LegacyPythonUDF {
    #[cfg(feature = "test-utils")]
    pub fn new_testing_udf() -> Self {
        Self {
            name: Arc::new("dummy_udf".to_string()),
            func: MaybeInitializedUDF::Uninitialized {
                inner: RuntimePyObject::new_none(),
                init_args: RuntimePyObject::new_none(),
            },
            bound_args: RuntimePyObject::new_none(),
            num_expressions: 1,
            return_dtype: DataType::Int64,
            resource_request: None,
            batch_size: None,
            concurrency: Some(NonZeroUsize::new(4).unwrap()),
            use_process: None,
            ray_options: None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn udf(
    name: &str,
    inner: RuntimePyObject,
    bound_args: RuntimePyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
    init_args: RuntimePyObject,
    resource_request: Option<ResourceRequest>,
    batch_size: Option<usize>,
    concurrency: Option<NonZeroUsize>,
    use_process: Option<bool>,
    ray_options: Option<RuntimePyObject>,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(LegacyPythonUDF {
            name: name.to_string().into(),
            func: MaybeInitializedUDF::Uninitialized { inner, init_args },
            bound_args,
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
            batch_size,
            concurrency,
            use_process,
            ray_options,
        }),
        inputs: expressions.into(),
    })
}

/// Generates a ResourceRequest by inspecting an iterator of expressions.
/// Looks for ResourceRequests on UDFs in each expression presented, and merges ResourceRequests across all expressions.
// TODO: Double-check if this is still needed with projects in Flotilla
pub fn get_resource_request<'a, E: Into<&'a ExprRef>>(
    exprs: impl IntoIterator<Item = E>,
) -> Option<ResourceRequest> {
    let merged_resource_requests = exprs
        .into_iter()
        .filter_map(|expr| {
            let mut resource_requests = Vec::new();
            expr.into()
                .apply(|e| match e.as_ref() {
                    Expr::Function {
                        func:
                            FunctionExpr::Python(LegacyPythonUDF {
                                resource_request, ..
                            }),
                        ..
                    } => {
                        if let Some(rr) = resource_request {
                            resource_requests.push(rr.clone());
                        }
                        Ok(TreeNodeRecursion::Continue)
                    }
                    _ => Ok(TreeNodeRecursion::Continue),
                })
                .unwrap();
            if resource_requests.is_empty() {
                None
            } else {
                Some(ResourceRequest::max_all(resource_requests.as_slice()))
            }
        })
        .collect_vec();
    if merged_resource_requests.is_empty() {
        None
    } else {
        Some(ResourceRequest::max_all(
            merged_resource_requests.as_slice(),
        ))
    }
}

#[cfg(feature = "python")]
fn py_udf_initialize(
    py: Python<'_>,
    func: Arc<Py<PyAny>>,
    init_args: Arc<Py<PyAny>>,
) -> DaftResult<Py<PyAny>> {
    Ok(func.call_method1(
        py,
        pyo3::intern!(py, "initialize"),
        (init_args.clone_ref(py),),
    )?)
}

/// Initializes all uninitialized UDFs in the expression
#[cfg(feature = "python")]
pub fn initialize_udfs(expr: ExprRef) -> DaftResult<ExprRef> {
    use common_treenode::Transformed;

    expr.transform(|e| match e.as_ref() {
        Expr::Function {
            func:
                FunctionExpr::Python(
                    python_udf @ LegacyPythonUDF {
                        func: MaybeInitializedUDF::Uninitialized { inner, init_args },
                        ..
                    },
                ),
            inputs,
        } => {
            let initialized_func = Python::attach(|py| {
                py_udf_initialize(py, inner.clone().unwrap(), init_args.clone().unwrap())
            })?;

            let initialized_expr = Expr::Function {
                func: FunctionExpr::Python(LegacyPythonUDF {
                    func: MaybeInitializedUDF::Initialized(initialized_func.into()),
                    ..python_udf.clone()
                }),
                inputs: inputs.clone(),
            };

            Ok(Transformed::yes(initialized_expr.into()))
        }
        _ => Ok(Transformed::no(e)),
    })
    .map(|transformed| transformed.data)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UDFProperties {
    pub name: String,
    pub resource_request: Option<ResourceRequest>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<NonZeroUsize>,
    pub use_process: Option<bool>,
    pub max_retries: Option<usize>,
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
                Expr::Function {
                    func:
                        FunctionExpr::Python(LegacyPythonUDF {
                            name,
                            resource_request,
                            batch_size,
                            concurrency,
                            use_process,
                            ray_options,
                            ..
                        }),
                    ..
                } => {
                    num_udfs += 1;
                    udf_properties = Some(Self {
                        name: name.as_ref().clone(),
                        resource_request: resource_request.clone(),
                        batch_size: *batch_size,
                        concurrency: *concurrency,
                        use_process: *use_process,
                        max_retries: None,
                        is_async: false,
                        on_error: None,
                        is_scalar: false,
                        ray_options: ray_options.clone(),
                    });
                }
                Expr::ScalarFn(ScalarFn::Python(PyScalarFn::RowWise(row_wise_fn))) => {
                    num_udfs += 1;
                    udf_properties = Some(Self {
                        name: row_wise_fn.function_name.to_string(),
                        resource_request: Some(ResourceRequest::try_new_internal(
                            None,
                            Some(row_wise_fn.gpus as f64),
                            None,
                        )?),
                        batch_size: None, // Row-wise functions don't have batch_size
                        concurrency: row_wise_fn.max_concurrency,
                        use_process: row_wise_fn.use_process,
                        max_retries: row_wise_fn.max_retries,
                        is_async: row_wise_fn.is_async,
                        on_error: Some(row_wise_fn.on_error),
                        is_scalar: false,
                        ray_options: None,
                    });
                }
                Expr::ScalarFn(ScalarFn::Python(PyScalarFn::Batch(BatchPyFn {
                    function_name,
                    gpus,
                    max_concurrency,
                    use_process,
                    batch_size,
                    max_retries,
                    on_error,
                    is_async,
                    ..
                }))) => {
                    num_udfs += 1;
                    udf_properties = Some(Self {
                        name: function_name.to_string(),
                        resource_request: Some(ResourceRequest::try_new_internal(
                            None,
                            Some(*gpus as f64),
                            None,
                        )?),
                        batch_size: *batch_size,
                        concurrency: *max_concurrency,
                        use_process: *use_process,
                        max_retries: *max_retries,
                        is_async: *is_async,
                        on_error: Some(*on_error),
                        is_scalar: false,
                        ray_options: None,
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
        self.concurrency.is_some()
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

        properties
    }
}
