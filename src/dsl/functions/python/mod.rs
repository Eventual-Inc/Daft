mod udf;

use std::collections::HashMap;

use indexmap::IndexMap;
use udf::PythonUDFEvaluator;
use serde::{Deserialize, Serialize, Serializer, Deserializer};
use pyo3::{PyObject, Python};
use crate::error::{DaftResult};

use crate::dsl::Expr;

use super::FunctionEvaluator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PyUdfInputType {
    PyList,
    Numpy,
    Pandas,
    PyArrow,
    Polars,
}

#[derive(Debug, Clone)]
pub struct SerializablePyObject(PyObject);

impl Serialize for SerializablePyObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Python::with_gil(|py| {
            // TODO: Call pickler
            todo!();
            // serializer.serialize_bytes(pickled_bytes)
        })
    }
}

impl<'de> Deserialize<'de> for SerializablePyObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        Python::with_gil(|py| {
            // TODO: Call depickling
            todo!();
        })
    }
}

impl<Rhs> PartialEq<Rhs> for SerializablePyObject {
    fn eq(&self, other: &Rhs) -> bool {
        Python::with_gil(|py| {
            // TODO: Call __eq__
            todo!();
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PyUdfInput {
    ExprAtIndex(usize, PyUdfInputType),
    PyValue(SerializablePyObject),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PythonExpr {
    PythonUDF {
        pyfunc: SerializablePyObject,
        args: Vec<PyUdfInput>,
        kwargs: IndexMap<String, PyUdfInput>,
    },
}

impl PythonExpr {
    #[inline]
    pub fn get_evaluator(&self) -> &dyn FunctionEvaluator {
        match self {
            PythonExpr::PythonUDF{pyfunc, args, kwargs} => &PythonUDFEvaluator {pyfunc: pyfunc.clone(), args: args.clone(), kwargs: kwargs.clone()},
        }
    }
}

pub fn udf(func: PyObject, arg_keys: &Vec<&str>, kwarg_keys: &Vec<&str>, expressions_map: &HashMap<&str, &Expr>, pyvalues_map: &HashMap<&str, PyObject>) -> DaftResult<Expr> {
    let mut expressions: Vec<Expr> = vec![];
    let mut parsed_args = vec![];
    let mut parsed_kwargs = IndexMap::new();

    for arg_key in arg_keys {
        if let Some(&e) = expressions_map.get(arg_key) {
            parsed_args.push(PyUdfInput::ExprAtIndex(expressions.len(), PyUdfInputType::PyList));
            expressions.push(e.clone());
        } else if let Some(pyobj) = pyvalues_map.get(arg_key) {
            parsed_args.push(PyUdfInput::PyValue(SerializablePyObject(pyobj.clone())));
        } else {
            panic!("Internal error occurred when constructing UDF")
        }
    }
    for kwarg_key in kwarg_keys {
        if let Some(&e) = expressions_map.get(kwarg_key) {
            parsed_kwargs.insert(kwarg_key.to_string(), PyUdfInput::ExprAtIndex(expressions.len(), PyUdfInputType::PyList));
            expressions.push(e.clone());
        } else if let Some(pyobj) = pyvalues_map.get(kwarg_key) {
            parsed_kwargs.insert(kwarg_key.to_string(), PyUdfInput::PyValue(SerializablePyObject(pyobj.clone())));
        } else {
            panic!("Internal error occurred when constructing UDF")
        }
    }

    Ok(Expr::Function {
        func: super::FunctionExpr::Python(PythonExpr::PythonUDF{
            pyfunc: SerializablePyObject(func.clone()),
            args: parsed_args,
            kwargs: parsed_kwargs,
        }),
        inputs: expressions,
    })
}
