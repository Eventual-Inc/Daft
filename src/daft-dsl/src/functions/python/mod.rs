mod runtime_py_object;
mod udf;
mod udf_runtime_binding;

#[cfg(feature = "python")]
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(feature = "python")]
use common_error::DaftError;
use common_error::DaftResult;
use common_resource_request::ResourceRequest;
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_core::datatypes::DataType;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::{Py, PyAny};
pub use runtime_py_object::RuntimePyObject;
use serde::{Deserialize, Serialize};
pub use udf_runtime_binding::UDFRuntimeBinding;

use super::{FunctionEvaluator, FunctionExpr};
use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PythonUDF {
    Stateless(StatelessPythonUDF),
    Stateful(StatefulPythonUDF),
}

impl PythonUDF {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            Self::Stateless(stateless_python_udf) => stateless_python_udf,
            Self::Stateful(stateful_python_udf) => stateful_python_udf,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StatelessPythonUDF {
    pub name: Arc<String>,
    partial_func: RuntimePyObject,
    num_expressions: usize,
    pub return_dtype: DataType,
    pub resource_request: Option<ResourceRequest>,
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StatefulPythonUDF {
    pub name: Arc<String>,
    pub stateful_partial_func: RuntimePyObject,
    pub num_expressions: usize,
    pub return_dtype: DataType,
    pub resource_request: Option<ResourceRequest>,
    pub init_args: Option<RuntimePyObject>,
    pub batch_size: Option<usize>,
    pub concurrency: Option<usize>,
    pub runtime_binding: UDFRuntimeBinding,
}

pub fn stateless_udf(
    name: &str,
    py_partial_stateless_udf: RuntimePyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
    resource_request: Option<ResourceRequest>,
    batch_size: Option<usize>,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
            name: name.to_string().into(),
            partial_func: py_partial_stateless_udf,
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
            batch_size,
        })),
        inputs: expressions.into(),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn stateful_udf(
    name: &str,
    py_stateful_partial_func: RuntimePyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
    resource_request: Option<ResourceRequest>,
    init_args: Option<RuntimePyObject>,
    batch_size: Option<usize>,
    concurrency: Option<usize>,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
            name: name.to_string().into(),
            stateful_partial_func: py_stateful_partial_func,
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
            init_args,
            batch_size,
            concurrency,
            runtime_binding: UDFRuntimeBinding::Unbound,
        })),
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
                        FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
                            resource_request,
                            ..
                        })),
                    ..
                }
                | Expr::Function {
                    func:
                        FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                            resource_request,
                            ..
                        })),
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

/// Gets the concurrency from the first StatefulUDF encountered in a given slice of expressions
///
/// NOTE: This function panics if no StatefulUDF is found
pub fn get_concurrency(exprs: &[ExprRef]) -> usize {
    let mut projection_concurrency = None;
    for expr in exprs {
        let mut found_stateful_udf = false;
        expr.apply(|e| match e.as_ref() {
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF{concurrency, ..})),
                ..
            } => {
                found_stateful_udf = true;
                projection_concurrency = Some(concurrency.expect("Should have concurrency specified"));
                Ok(common_treenode::TreeNodeRecursion::Stop)
            }
            _ => Ok(common_treenode::TreeNodeRecursion::Continue),
        }).unwrap();
        if found_stateful_udf {
            break;
        }
    }
    projection_concurrency.expect("get_concurrency expects one StatefulUDF")
}

/// Binds every StatefulPythonUDF expression to an initialized function provided by an actor
#[cfg(feature = "python")]
pub fn bind_stateful_udfs(
    expr: ExprRef,
    initialized_funcs: &HashMap<String, Py<PyAny>>,
) -> DaftResult<ExprRef> {
    expr.transform(|e| match e.as_ref() {
        Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(stateful_py_udf)),
            inputs,
        } => {
            let f = initialized_funcs
                .get(stateful_py_udf.name.as_ref())
                .ok_or_else(|| {
                    DaftError::InternalError(format!(
                        "Unable to find UDF to bind: {}",
                        stateful_py_udf.name.as_ref()
                    ))
                })?;
            let bound_expr = Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                    runtime_binding: udf_runtime_binding::UDFRuntimeBinding::Bound(
                        f.clone().into(),
                    ),
                    ..stateful_py_udf.clone()
                })),
                inputs: inputs.clone(),
            };
            Ok(common_treenode::Transformed::yes(bound_expr.into()))
        }
        _ => Ok(common_treenode::Transformed::no(e)),
    })
    .map(|transformed| transformed.data)
}

/// Helper function that extracts all PartialStatefulUDF python objects from a given expression tree
#[cfg(feature = "python")]
pub fn extract_partial_stateful_udf_py(
    expr: ExprRef,
) -> HashMap<String, (Py<PyAny>, Option<Py<PyAny>>)> {
    extract_stateful_udf_exprs(expr)
        .into_iter()
        .map(|stateful_udf| {
            (
                stateful_udf.name.as_ref().to_string(),
                (
                    stateful_udf.stateful_partial_func.as_ref().clone(),
                    stateful_udf.init_args.map(|x| x.as_ref().clone()),
                ),
            )
        })
        .collect()
}

/// Helper function that extracts all `StatefulPythonUDF` expressions from a given expression tree
pub fn extract_stateful_udf_exprs(expr: ExprRef) -> Vec<StatefulPythonUDF> {
    let mut stateful_udf_exprs = Vec::new();

    expr.apply(|child| {
        if let Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(stateful_udf)),
            ..
        } = child.as_ref()
        {
            stateful_udf_exprs.push(stateful_udf.clone());
        }

        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    stateful_udf_exprs
}
