#[cfg(feature = "python")]
mod pyobj_serde;
mod udf;
#[cfg(feature = "python")]
mod udf_runtime_binding;

use std::{collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_resource_request::ResourceRequest;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_core::datatypes::DataType;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::{FunctionEvaluator, FunctionExpr};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PythonUDF {
    Stateless(StatelessPythonUDF),
    Stateful(StatefulPythonUDF),
}

impl PythonUDF {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            PythonUDF::Stateless(stateless_python_udf) => stateless_python_udf,
            PythonUDF::Stateful(stateful_python_udf) => stateful_python_udf,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StatelessPythonUDF {
    pub name: Arc<String>,
    #[cfg(feature = "python")]
    partial_func: pyobj_serde::PyObjectWrapper,
    num_expressions: usize,
    pub return_dtype: DataType,
    pub resource_request: Option<ResourceRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StatefulPythonUDF {
    pub name: Arc<String>,
    #[cfg(feature = "python")]
    pub stateful_partial_func: pyobj_serde::PyObjectWrapper,
    pub num_expressions: usize,
    pub return_dtype: DataType,
    pub resource_request: Option<ResourceRequest>,
    #[cfg(feature = "python")]
    pub init_args: Option<pyobj_serde::PyObjectWrapper>,
    #[cfg(feature = "python")]
    pub runtime_binding: udf_runtime_binding::UDFRuntimeBinding,
}

#[cfg(feature = "python")]
pub fn stateless_udf(
    name: &str,
    py_partial_stateless_udf: pyo3::PyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
    resource_request: Option<ResourceRequest>,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
            name: name.to_string().into(),
            partial_func: pyobj_serde::PyObjectWrapper(py_partial_stateless_udf),
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
        })),
        inputs: expressions.into(),
    })
}

#[cfg(not(feature = "python"))]
pub fn stateless_udf(
    name: &str,
    expressions: &[ExprRef],
    return_dtype: DataType,
    resource_request: Option<ResourceRequest>,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
            name: name.to_string().into(),
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
        })),
        inputs: expressions.into(),
    })
}

#[cfg(feature = "python")]
pub fn stateful_udf(
    name: &str,
    py_stateful_partial_func: pyo3::PyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
    resource_request: Option<ResourceRequest>,
    init_args: Option<pyo3::PyObject>,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
            name: name.to_string().into(),
            stateful_partial_func: pyobj_serde::PyObjectWrapper(py_stateful_partial_func),
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
            init_args: init_args.map(pyobj_serde::PyObjectWrapper),
            runtime_binding: udf_runtime_binding::UDFRuntimeBinding::Unbound,
        })),
        inputs: expressions.into(),
    })
}

#[cfg(not(feature = "python"))]
pub fn stateful_udf(
    name: &str,
    expressions: &[ExprRef],
    return_dtype: DataType,
    resource_request: Option<ResourceRequest>,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
            name: name.to_string().into(),
            num_expressions: expressions.len(),
            return_dtype,
            resource_request,
        })),
        inputs: expressions.into(),
    })
}

/// Replaces resource_requests on UDF expressions in the provided expression tree
pub fn replace_udf_resource_request(
    expr: ExprRef,
    override_resource_request: &ResourceRequest,
) -> ExprRef {
    expr.transform(|e| match e.as_ref() {
        Expr::Function {
            func:
                FunctionExpr::Python(PythonUDF::Stateful(
                    original @ StatefulPythonUDF {
                        resource_request, ..
                    },
                )),
            inputs,
        } => {
            if let Some(existing_rr) = resource_request
                && existing_rr == override_resource_request
            {
                return Ok(Transformed::no(e));
            }
            let new_expr = Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                    resource_request: Some(override_resource_request.clone()),
                    ..original.clone()
                })),
                inputs: inputs.clone(),
            };
            Ok(Transformed::yes(new_expr.arced()))
        }
        Expr::Function {
            func:
                FunctionExpr::Python(PythonUDF::Stateless(
                    original @ StatelessPythonUDF {
                        resource_request, ..
                    },
                )),
            inputs,
        } => {
            if let Some(existing_rr) = resource_request
                && existing_rr == override_resource_request
            {
                return Ok(Transformed::no(e));
            }
            let new_expr = Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
                    resource_request: Some(override_resource_request.clone()),
                    ..original.clone()
                })),
                inputs: inputs.clone(),
            };
            Ok(Transformed::yes(new_expr.arced()))
        }
        _ => Ok(Transformed::no(e)),
    })
    .unwrap()
    .data
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
                        resource_requests.push(rr.clone())
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

/// Binds every StatefulPythonUDF expression to an initialized function provided by an actor
#[cfg(feature = "python")]
pub fn bind_stateful_udfs(
    expr: ExprRef,
    initialized_funcs: &HashMap<String, pyo3::Py<pyo3::PyAny>>,
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
                    runtime_binding: udf_runtime_binding::UDFRuntimeBinding::Bound(f.clone()),
                    ..stateful_py_udf.clone()
                })),
                inputs: inputs.clone(),
            };
            Ok(Transformed::yes(bound_expr.into()))
        }
        _ => Ok(Transformed::no(e)),
    })
    .map(|transformed| transformed.data)
}

/// Helper function that extracts all PartialStatefulUDF python objects from a given expression tree
#[cfg(feature = "python")]
pub fn extract_partial_stateful_udf_py(expr: ExprRef) -> HashMap<String, pyo3::Py<pyo3::PyAny>> {
    let mut py_partial_udfs = HashMap::new();
    expr.apply(|child| {
        if let Expr::Function {
            func:
                FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                    name,
                    stateful_partial_func: py_partial_udf,
                    ..
                })),
            ..
        } = child.as_ref()
        {
            py_partial_udfs.insert(name.as_ref().to_string(), py_partial_udf.0.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    py_partial_udfs
}
