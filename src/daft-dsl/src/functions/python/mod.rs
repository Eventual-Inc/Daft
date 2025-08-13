mod runtime_py_object;
mod udf;

use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_core::prelude::*;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::{
    types::{PyDict, PyTuple},
    Bound, IntoPyObject, PyObject, PyResult, Python,
};
pub use runtime_py_object::RuntimePyObject;
use serde::{Deserialize, Serialize};

use super::FunctionExpr;
#[cfg(feature = "python")]
use crate::python::PyExpr;
use crate::{functions::scalar::ScalarFn, Expr, ExprRef};

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
    pub inner: Arc<PyObject>,
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
        A: IntoPyObject<'py, Target = PyTuple>,
    {
        let o = self.inner.call(py, args, kwargs)?;
        let inner = o.getattr(py, "_expr")?;

        let expr = inner.extract::<PyExpr>(py)?;
        Ok(expr)
    }

    pub fn name(&self) -> PyResult<String> {
        Python::with_gil(|py| {
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
    pub concurrency: Option<usize>,
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
            concurrency: Some(4),
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
    concurrency: Option<usize>,
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

/// Gets the concurrency from the first UDF encountered in a given slice of expressions
///
/// NOTE: This function panics if no UDF is found or if the first UDF has no concurrency
pub fn get_concurrency<'a, E: Into<&'a ExprRef>>(exprs: impl IntoIterator<Item = E>) -> usize {
    let mut projection_concurrency = None;
    for expr in exprs {
        let mut found_actor_pool_udf = false;
        expr.into()
            .apply(|e| match e.as_ref() {
                Expr::Function {
                    func: FunctionExpr::Python(LegacyPythonUDF { concurrency, .. }),
                    ..
                } => {
                    found_actor_pool_udf = true;
                    projection_concurrency =
                        Some(concurrency.expect("Should have concurrency specified"));
                    Ok(common_treenode::TreeNodeRecursion::Stop)
                }
                _ => Ok(common_treenode::TreeNodeRecursion::Continue),
            })
            .unwrap();
        if found_actor_pool_udf {
            break;
        }
    }
    projection_concurrency.expect("get_concurrency expects one UDF with concurrency set")
}

pub fn try_get_concurrency(expr: &ExprRef) -> Option<usize> {
    let mut projection_concurrency = None;
    expr.apply(|e| match e.as_ref() {
        Expr::Function {
            func: FunctionExpr::Python(LegacyPythonUDF { concurrency, .. }),
            ..
        } => {
            projection_concurrency = *concurrency;
            Ok(common_treenode::TreeNodeRecursion::Stop)
        }
        _ => Ok(common_treenode::TreeNodeRecursion::Continue),
    })
    .unwrap();

    projection_concurrency
}

/// Gets the batch size from the first UDF encountered in a given slice of expressions
/// Errors if no UDF is found
pub fn try_get_batch_size_from_udf(expr: &ExprRef) -> DaftResult<Option<usize>> {
    let mut projection_batch_size = None;
    expr.apply(|e| match e.as_ref() {
        Expr::Function {
            func: FunctionExpr::Python(LegacyPythonUDF { batch_size, .. }),
            ..
        } => {
            projection_batch_size = Some(*batch_size);
            Ok(common_treenode::TreeNodeRecursion::Stop)
        }
        _ => Ok(common_treenode::TreeNodeRecursion::Continue),
    })
    .unwrap();

    if let Some(batch_size) = projection_batch_size {
        Ok(batch_size)
    } else {
        Err(DaftError::ValueError(format!(
            "No UDF with batch size found in expression: {:?}",
            expr
        )))
    }
}

pub fn get_use_process(expr: &ExprRef) -> DaftResult<Option<bool>> {
    let mut finder = None;
    expr.apply(|e| match e.as_ref() {
        Expr::Function {
            func: FunctionExpr::Python(LegacyPythonUDF { use_process, .. }),
            ..
        } => {
            finder = Some(*use_process);
            Ok(common_treenode::TreeNodeRecursion::Stop)
        }
        _ => Ok(common_treenode::TreeNodeRecursion::Continue),
    })
    .unwrap();

    if let Some(finder) = finder {
        Ok(finder)
    } else {
        Err(DaftError::ValueError(format!(
            "No UDF with use_process found in expression: {:?}",
            expr
        )))
    }
}

#[cfg(feature = "python")]
fn py_udf_initialize(
    py: Python<'_>,
    func: Arc<pyo3::PyObject>,
    init_args: Arc<pyo3::PyObject>,
) -> DaftResult<pyo3::PyObject> {
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
            let initialized_func = Python::with_gil(|py| {
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

/// Get the names of all UDFs in expression
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
        }

        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    udf_name
}

/// UDF name and settings
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UDFProperties {
    pub name: String,
    pub resource_request: Option<ResourceRequest>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<usize>,
    pub use_process: Option<bool>,
}

impl UDFProperties {
    pub fn is_actor_pool_udf(&self) -> bool {
        self.concurrency.is_some()
    }
}

pub fn get_udf_properties(expr: &ExprRef) -> UDFProperties {
    let mut udf_properties = None;

    expr.apply(|e| {
        if let Expr::Function {
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
        } = e.as_ref()
        {
            udf_properties = Some(UDFProperties {
                name: name.as_ref().clone(),
                resource_request: resource_request.clone(),
                batch_size: *batch_size,
                concurrency: *concurrency,
                use_process: *use_process,
            });
        } else if let Expr::ScalarFn(ScalarFn::Python(py)) = e.as_ref() {
            udf_properties = Some(UDFProperties {
                name: py.name().to_string(),
                resource_request: None,
                batch_size: Some(512),
                concurrency: None,
                use_process: None,
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    udf_properties.expect("get_udf_properties expects exactly one UDF in expression")
}
