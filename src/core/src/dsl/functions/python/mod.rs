mod partial_udf;
mod udf;

use crate::datatypes::DataType;
use common_error::DaftResult;
use serde::{Deserialize, Serialize};

use crate::dsl::Expr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PythonUDF {
    func: partial_udf::PartialUDF,
    num_expressions: usize,
    return_dtype: DataType,
}

pub fn udf(func: pyo3::PyObject, expressions: &[Expr], return_dtype: DataType) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF {
            func: partial_udf::PartialUDF(func),
            num_expressions: expressions.len(),
            return_dtype,
        }),
        inputs: expressions.into(),
    })
}
