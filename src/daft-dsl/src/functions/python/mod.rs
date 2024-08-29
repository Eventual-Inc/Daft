mod udf;

use std::sync::Arc;

use common_error::DaftResult;
use common_resource_request::ResourceRequest;
use common_treenode::{TreeNode, TreeNodeRecursion};
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

/// A wrapper around PyObject that is safe to use even when the Python feature flag isn't turned on
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuntimePyObject {
    #[cfg(feature = "python")]
    obj: crate::pyobj_serde::PyObjectWrapper,
}

impl RuntimePyObject {
    #[cfg(feature = "test-utils")]
    pub fn new_testing_none() -> Self {
        #[cfg(feature = "python")]
        {
            let none_value = pyo3::Python::with_gil(|py| py.None());
            Self {
                obj: crate::pyobj_serde::PyObjectWrapper(none_value),
            }
        }
        #[cfg(not(feature = "python"))]
        {
            Self {}
        }
    }

    #[cfg(feature = "python")]
    pub fn new(value: pyo3::PyObject) -> Self {
        Self {
            obj: crate::pyobj_serde::PyObjectWrapper(value),
        }
    }

    #[cfg(feature = "python")]
    pub fn unwrap(&self) -> &pyo3::PyObject {
        &self.obj.0
    }
}

#[cfg(feature = "python")]
impl From<pyo3::PyObject> for RuntimePyObject {
    fn from(value: pyo3::PyObject) -> Self {
        Self::new(value)
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

/// Gets the concurrency from the first StatefulUDF encountered in a given slice of expressions
///
/// NOTE: This function panics if no StatefulUDF is found
pub fn get_concurrency(exprs: &[ExprRef]) -> usize {
    let mut projection_concurrency = None;
    for expr in exprs.iter() {
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
