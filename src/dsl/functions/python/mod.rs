mod udf;

use crate::error::DaftResult;
use pyo3::{PyObject, Python};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::dsl::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone)]
pub struct SerializablePyObject(PyObject);

impl Serialize for SerializablePyObject {
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

impl<'de> Deserialize<'de> for SerializablePyObject {
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

impl<Rhs> PartialEq<Rhs> for SerializablePyObject {
    fn eq(&self, _other: &Rhs) -> bool {
        Python::with_gil(|_py| {
            // TODO: Call __eq__
            todo!();
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PyUdfInput {
    ExprAtIndex(usize),
    PyValue(SerializablePyObject),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PythonExpr {
    PythonUDF { pyfunc: SerializablePyObject },
}

impl PythonExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            PythonExpr::PythonUDF { .. } => self,
        }
    }
}

pub fn udf(func: PyObject, expressions: &[Expr]) -> DaftResult<Expr> {
    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonExpr::PythonUDF {
            pyfunc: SerializablePyObject(func),
        }),
        inputs: expressions.into(),
    })
}
