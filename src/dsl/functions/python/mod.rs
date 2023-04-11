mod udf;

use crate::dsl;
use crate::{dsl::lit::LiteralValue, error::DaftResult};
use indexmap::IndexMap;
use pyo3::{PyObject, Python};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

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
    LitValue(LiteralValue),
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
            PythonExpr::PythonUDF { .. } => self,
        }
    }
}

pub fn udf(
    func: PyObject,
    arg_keys: &Vec<&str>,
    kwarg_keys: &Vec<&str>,
    expressions_map: &HashMap<&str, &dsl::Expr>,
    pyvalues_literal_map: &HashMap<&str, LiteralValue>,
) -> DaftResult<dsl::Expr> {
    let mut expressions: Vec<dsl::Expr> = vec![];
    let mut parsed_args = vec![];
    let mut parsed_kwargs = IndexMap::new();

    for arg_key in arg_keys {
        if let Some(&e) = expressions_map.get(arg_key) {
            parsed_args.push(PyUdfInput::ExprAtIndex(expressions.len()));
            expressions.push(e.clone());
        } else if let Some(literal) = pyvalues_literal_map.get(arg_key) {
            parsed_args.push(PyUdfInput::LitValue(literal.to_owned()));
        } else {
            panic!("Internal error occurred when constructing UDF")
        }
    }
    for kwarg_key in kwarg_keys {
        if let Some(&e) = expressions_map.get(kwarg_key) {
            parsed_kwargs.insert(
                kwarg_key.to_string(),
                PyUdfInput::ExprAtIndex(expressions.len()),
            );
            expressions.push(e.clone());
        } else if let Some(literal) = pyvalues_literal_map.get(kwarg_key) {
            parsed_kwargs.insert(
                kwarg_key.to_string(),
                PyUdfInput::LitValue(literal.to_owned()),
            );
        } else {
            panic!("Internal error occurred when constructing UDF")
        }
    }

    Ok(dsl::Expr::Function {
        func: super::FunctionExpr::Python(PythonExpr::PythonUDF {
            pyfunc: SerializablePyObject(func),
            args: parsed_args,
            kwargs: parsed_kwargs,
        }),
        inputs: expressions,
    })
}
