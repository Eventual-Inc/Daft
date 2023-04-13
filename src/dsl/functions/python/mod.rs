mod udf;

use crate::{datatypes::DataType, error::DaftResult};
use pyo3::{PyObject, Python};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::dsl::Expr;

// A curried Python function that takes as input a list of Series objects for execution
#[derive(Debug, Clone)]
pub struct PartialUDF(PyObject);

impl Serialize for PartialUDF {
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

impl<'de> Deserialize<'de> for PartialUDF {
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

impl<Rhs> PartialEq<Rhs> for PartialUDF {
    fn eq(&self, _other: &Rhs) -> bool {
        Python::with_gil(|_py| {
            // TODO: Call __eq__
            todo!();
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PythonUDF {
    func: PartialUDF,
    num_expressions: usize,
    return_dtype: DataType,
}

pub fn udf(func: PyObject, expressions: &[Expr], return_dtype: DataType) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonUDF {
            func: PartialUDF(func),
            num_expressions: expressions.len(),
            return_dtype,
        }),
        inputs: expressions.into(),
    })
}
