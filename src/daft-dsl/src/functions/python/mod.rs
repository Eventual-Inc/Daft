#[cfg(feature = "python")]
mod partial_udf;
mod udf;

use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_core::datatypes::DataType;
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
    partial_func: partial_udf::PyPartialUDF,
    num_expressions: usize,
    pub return_dtype: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StatefulPythonUDF {
    pub name: Arc<String>,
    #[cfg(feature = "python")]
    pub stateful_partial_func: partial_udf::PyPartialUDF,
    pub num_expressions: usize,
    pub return_dtype: DataType,
}

#[cfg(feature = "python")]
pub fn stateless_udf(
    name: &str,
    py_partial_stateless_udf: pyo3::PyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
            name: name.to_string().into(),
            partial_func: partial_udf::PyPartialUDF(py_partial_stateless_udf),
            num_expressions: expressions.len(),
            return_dtype,
        })),
        inputs: expressions.into(),
    })
}

#[cfg(not(feature = "python"))]
pub fn stateless_udf(
    name: &str,
    expressions: &[ExprRef],
    return_dtype: DataType,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
            name: name.to_string().into(),
            num_expressions: expressions.len(),
            return_dtype,
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
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
            name: name.to_string().into(),
            stateful_partial_func: partial_udf::PyPartialUDF(py_stateful_partial_func),
            num_expressions: expressions.len(),
            return_dtype,
        })),
        inputs: expressions.into(),
    })
}

#[cfg(not(feature = "python"))]
pub fn stateful_udf(
    name: &str,
    expressions: &[ExprRef],
    return_dtype: DataType,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
            name: name.to_string().into(),
            num_expressions: expressions.len(),
            return_dtype,
        })),
        inputs: expressions.into(),
    })
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
