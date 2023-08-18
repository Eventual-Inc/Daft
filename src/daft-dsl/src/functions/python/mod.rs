mod partial_udf;
mod udf;

use common_error::DaftResult;
use daft_core::datatypes::DataType;
use serde::{Deserialize, Serialize};

use crate::Expr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
