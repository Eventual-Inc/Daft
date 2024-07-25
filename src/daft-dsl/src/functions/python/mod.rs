mod partial_udf;
mod udf;

use common_error::DaftResult;
use daft_core::datatypes::DataType;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

use super::FunctionEvaluator;

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
    partial_func: partial_udf::PyPartialUDF,
    num_expressions: usize,
    pub return_dtype: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StatefulPythonUDF {
    stateful_partial_func: partial_udf::PyPartialUDF,
    num_expressions: usize,
    pub return_dtype: DataType,
}

pub fn stateless_udf(
    py_partial_stateless_udf: pyo3::PyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateless(StatelessPythonUDF {
            partial_func: partial_udf::PyPartialUDF(py_partial_stateless_udf),
            num_expressions: expressions.len(),
            return_dtype,
        })),
        inputs: expressions.into(),
    })
}

pub fn stateful_udf(
    py_stateful_partial_func: pyo3::PyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
            stateful_partial_func: partial_udf::PyPartialUDF(py_stateful_partial_func),
            num_expressions: expressions.len(),
            return_dtype,
        })),
        inputs: expressions.into(),
    })
}
