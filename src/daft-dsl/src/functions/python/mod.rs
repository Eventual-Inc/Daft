mod partial_udf;
mod udf;

use common_error::DaftResult;
use daft_core::datatypes::DataType;
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PythonUDF {
    func: partial_udf::PartialUDF,
    num_expressions: usize,
    pub return_dtype: DataType,
    pub stateful: bool,
}

pub fn udf(
    func: pyo3::PyObject,
    expressions: &[ExprRef],
    return_dtype: DataType,
    stateful: bool,
) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF {
            func: partial_udf::PartialUDF(func),
            num_expressions: expressions.len(),
            return_dtype,
            stateful,
        }),
        inputs: expressions.into(),
    })
}
