mod runtime_py_object;
mod udf;

use std::{num::NonZeroUsize, str::FromStr, sync::Arc};

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
    python_udf::{BatchPyFn, PyScalarFn, RowWisePyFn},
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
        }),
        inputs: expressions.into(),
    })
}

/// Generates a ResourceRequest by inspecting an iterator of expressions.
/// Looks for ResourceRequests on UDFs in each expression presented, and merges ResourceRequests across all expressions.
// TODO: Remove with the old Ray Runner
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

/// Get the names of all UDFs in expression
// TODO: Remove with the old Ray Runner
pub fn try_get_udf_name(expr: &ExprRef) -> Option<String> {
    let mut udf_name = None;

    expr.apply(|e| {
        if let Expr::Function {
            func: FunctionExpr::Python(LegacyPythonUDF { name, .. }),
            ..
        } = e.as_ref()
        {
            udf_name = Some(name.as_ref().clone());
            return Ok(TreeNodeRecursion::Stop);
        } else if let Expr::ScalarFn(ScalarFn::Python(py)) = e.as_ref() {
            udf_name = Some(py.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    udf_name
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
                    });
                }
                Expr::ScalarFn(ScalarFn::Python(PyScalarFn::RowWise(RowWisePyFn {
                    function_name,
                    gpus,
                    max_concurrency,
                    use_process,
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
                        batch_size: None,
                        concurrency: *max_concurrency,
                        use_process: *use_process,
                        max_retries: *max_retries,
                        is_async: *is_async,
                        on_error: Some(*on_error),
                        is_scalar: true,
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
}
