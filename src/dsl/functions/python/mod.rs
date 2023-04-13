mod udf;

use crate::{datatypes::DataType, error::DaftResult};
use pyo3::{
    types::{PyBytes, PyModule},
    PyObject, Python, ToPyObject,
};
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::dsl::Expr;

// A curried Python function that takes as input a list of Series objects for execution
#[derive(Debug, Clone)]
pub struct PartialUDF(PyObject);

impl Serialize for PartialUDF {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // NOTE: because this code needs to work with any Serializer and its associated S::Error type,
        // we resort to panicking when a PyErr occurs and cannot correctly return the appropriate Error
        Python::with_gil(|py| {
            let serde_module = PyModule::import(py, "daft.serialization").unwrap();
            let serialize = serde_module.getattr("serialize").unwrap();
            let pybytes = serialize
                .call1((self.0.clone_ref(py).into_ref(py),))
                .unwrap();
            serializer.serialize_bytes(pybytes.downcast::<PyBytes>().unwrap().as_bytes())
        })
    }
}

struct PartialUDFVisitor;

impl<'de> Visitor<'de> for PartialUDFVisitor {
    type Value = PartialUDF;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Python object of type PartialUDF")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // NOTE: because this code needs to work with any Deserializer and its associated D::Error type,
        // we resort to panicking when a PyErr occurs and cannot correctly return the appropriate Error
        Python::with_gil(|py| {
            let serde_module = PyModule::import(py, "daft.serialization").unwrap();
            let deserialize = serde_module.getattr("deserialize").unwrap();
            let py_partial_udf = deserialize.call1((v,)).unwrap();
            Ok(PartialUDF(py_partial_udf.to_object(py)))
        })
    }
}

impl<'de> Deserialize<'de> for PartialUDF {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PartialUDFVisitor)
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
