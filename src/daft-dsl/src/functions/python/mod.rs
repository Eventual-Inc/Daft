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
use crate::{functions::scalar::ScalarFn, python_udf::PyScalarFn, Expr, ExprRef};

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
        }

        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();

    udf_name
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum UDFImpl {
    Legacy(LegacyPythonUDF, Vec<ExprRef>),
    PyScalarFn(PyScalarFn),
}

impl UDFImpl {
    pub fn from_expr(expr: &ExprRef) -> DaftResult<Self> {
        match expr.as_ref() {
            Expr::Function {
                func: FunctionExpr::Python(func),
                inputs: input_exprs,
            } => Ok(Self::Legacy(func.clone(), input_exprs.clone())),
            Expr::ScalarFn(ScalarFn::Python(f)) => Ok(Self::PyScalarFn(f.clone())),
            _ => Err(DaftError::InternalError(format!(
                "Expected a Python UDF, got {}",
                expr
            ))),
        }
    }

    pub fn to_expr(&self) -> Arc<Expr> {
        match self {
            Self::Legacy(udf, inputs) => Expr::Function {
                func: FunctionExpr::Python(udf.clone()),
                inputs: inputs.clone(),
            }
            .arced(),
            Self::PyScalarFn(udf) => Expr::ScalarFn(ScalarFn::Python(udf.clone())).arced(),
        }
    }

    pub fn to_field(&self, schema: &SchemaRef) -> DaftResult<Field> {
        match self {
            Self::Legacy(udf, inputs) => match inputs.as_slice() {
                [] => Err(DaftError::ValueError(
                    "Cannot run UDF with 0 expression arguments".into(),
                )),
                [first, ..] => Ok(Field::new(first.name(), udf.return_dtype.clone())),
            },
            Self::PyScalarFn(udf) => udf.to_field(schema),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Legacy(udf, _) => udf.name.as_ref(),
            Self::PyScalarFn(udf) => udf.name(),
        }
    }

    pub fn args(&self) -> Vec<ExprRef> {
        match self {
            Self::Legacy(_, inputs) => inputs.clone(),
            Self::PyScalarFn(udf) => udf.args(),
        }
    }

    pub fn is_actor_pool_udf(&self) -> bool {
        if let Self::Legacy(udf, _) = self {
            udf.concurrency.is_some()
        } else {
            false
        }
    }

    pub fn batch_size(&self) -> Option<usize> {
        if let Self::Legacy(udf, _) = self {
            udf.batch_size
        } else {
            Some(512)
        }
    }

    pub fn concurrency(&self) -> Option<usize> {
        if let Self::Legacy(udf, _) = self {
            udf.concurrency
        } else {
            None
        }
    }

    pub fn use_process(&self) -> Option<bool> {
        if let Self::Legacy(udf, _) = self {
            udf.use_process
        } else {
            None
        }
    }

    pub fn resource_request(&self) -> Option<&ResourceRequest> {
        if let Self::Legacy(udf, _) = self {
            udf.resource_request.as_ref()
        } else {
            None
        }
    }
}
