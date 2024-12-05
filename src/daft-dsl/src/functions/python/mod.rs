mod runtime_py_object;
mod udf;

use std::sync::Arc;

use common_error::DaftResult;
use common_resource_request::ResourceRequest;
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_core::prelude::*;
use itertools::Itertools;
pub use runtime_py_object::RuntimePyObject;
use serde::{Deserialize, Serialize};

use super::FunctionExpr;
use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MaybeInitializedUDF {
    Initialized(RuntimePyObject),
    Uninitialized {
        inner: RuntimePyObject,
        init_args: RuntimePyObject,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PythonUDF {
    pub name: Arc<String>,
    pub func: MaybeInitializedUDF,
    pub bound_args: RuntimePyObject,
    pub num_expressions: usize,
    pub return_dtype: DataType,
    pub resource_request: Option<ResourceRequest>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<usize>,
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
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF {
            name: name.to_string().into(),
            func: MaybeInitializedUDF::Uninitialized { inner, init_args },
            bound_args,
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
            batch_size,
            concurrency,
        }),
        inputs: expressions.into(),
    })
}

/// Generates a ResourceRequest by inspecting an iterator of expressions.
/// Looks for ResourceRequests on UDFs in each expression presented, and merges ResourceRequests across all expressions.
pub fn get_resource_request(exprs: &[ExprRef]) -> Option<ResourceRequest> {
    let merged_resource_requests = exprs
        .iter()
        .filter_map(|expr| {
            let mut resource_requests = Vec::new();
            expr.apply(|e| match e.as_ref() {
                Expr::Function {
                    func:
                        FunctionExpr::Python(PythonUDF {
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
pub fn get_concurrency(exprs: &[ExprRef]) -> usize {
    let mut projection_concurrency = None;
    for expr in exprs {
        let mut found_actor_pool_udf = false;
        expr.apply(|e| match e.as_ref() {
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF { concurrency, .. }),
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

/// Gets the batch size from the first UDF encountered in a given slice of expressions
pub fn get_batch_size(exprs: &[ExprRef]) -> Option<usize> {
    let mut projection_batch_size = None;
    for expr in exprs {
        let mut found_udf = false;
        expr.apply(|e| match e.as_ref() {
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF { batch_size, .. }),
                ..
            } => {
                found_udf = true;
                projection_batch_size = Some(*batch_size);
                Ok(common_treenode::TreeNodeRecursion::Stop)
            }
            _ => Ok(common_treenode::TreeNodeRecursion::Continue),
        })
        .unwrap();
        if found_udf {
            break;
        }
    }
    projection_batch_size.expect("get_batch_size expects one UDF")
}

#[cfg(feature = "python")]
fn py_udf_initialize(
    func: pyo3::PyObject,
    init_args: pyo3::PyObject,
) -> DaftResult<pyo3::PyObject> {
    use pyo3::Python;

    Ok(Python::with_gil(move |py| {
        func.call_method1(py, pyo3::intern!(py, "initialize"), (init_args,))
    })?)
}

/// Initializes all uninitialized UDFs in the expression
#[cfg(feature = "python")]
pub fn initialize_udfs(expr: ExprRef) -> DaftResult<ExprRef> {
    use common_treenode::Transformed;

    expr.transform(|e| match e.as_ref() {
        Expr::Function {
            func:
                FunctionExpr::Python(
                    python_udf @ PythonUDF {
                        func: MaybeInitializedUDF::Uninitialized { inner, init_args },
                        ..
                    },
                ),
            inputs,
        } => {
            let initialized_func =
                py_udf_initialize(inner.clone().unwrap(), init_args.clone().unwrap())?;

            let initialized_expr = Expr::Function {
                func: FunctionExpr::Python(PythonUDF {
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
pub fn get_udf_names(expr: &ExprRef) -> Vec<String> {
    let mut names = Vec::new();

    expr.apply(|e| {
        if let Expr::Function {
            func: FunctionExpr::Python(PythonUDF { name, .. }),
            ..
        } = e.as_ref()
        {
            names.push(name.to_string());
        }

        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    names
}
