mod udf;

use crate::error::DaftResult;
use pyo3::{PyObject, Python};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::dsl::Expr;

// A curried Python function that takes as input a list of Series objects for execution
#[derive(Debug, Clone)]
pub struct PythonPartialUDFFunction(PyObject);

impl Serialize for PythonPartialUDFFunction {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Python::with_gil(|_py| {
            // TODO: Call pickler
            todo!();
        })
    }
}

impl<'de> Deserialize<'de> for PythonPartialUDFFunction {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Python::with_gil(|_py| {
            // TODO: Call depickling
            todo!();
        })
    }
}

impl<Rhs> PartialEq<Rhs> for PythonPartialUDFFunction {
    fn eq(&self, _other: &Rhs) -> bool {
        Python::with_gil(|_py| {
            // TODO: Call __eq__
            todo!();
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PythonUDF(PythonPartialUDFFunction);

pub fn udf(func: PyObject, expressions: &[Expr]) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF(PythonPartialUDFFunction(func))),
        inputs: expressions.into(),
    })
}
