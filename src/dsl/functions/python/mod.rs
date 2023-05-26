mod partial_udf;
mod udf;

use crate::{datatypes::DataType, error::DaftResult};
use serde::{Deserialize, Serialize};

use super::FunctionEvaluator;
use crate::dsl::Expr;
use udf::PythonUDFEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PythonUDFExpr {
    Python(partial_udf::PartialUDF, usize, DataType),
}

impl PythonUDFExpr {
    #[inline]
    pub fn get_evaluator(&self) -> Box<dyn FunctionEvaluator> {
        use PythonUDFExpr::*;
        match self {
            Python(func, num_expressions, return_dtype) => Box::new(PythonUDFEvaluator {
                func: func.clone(),
                num_expressions: *num_expressions,
                return_dtype: return_dtype.clone(),
            }),
        }
    }
}

pub fn udf(func: pyo3::PyObject, expressions: &[Expr], return_dtype: DataType) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDFExpr::Python(
            partial_udf::PartialUDF(func),
            expressions.len(),
            return_dtype,
        )),
        inputs: expressions.into(),
    })
}
