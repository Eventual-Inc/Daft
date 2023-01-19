use std::sync::Arc;

use crate::dsl::{self, Expr};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    pyclass::CompareOp,
    types::{PyBool, PyFloat, PyInt, PyString},
};

#[pyfunction]
pub fn col(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr::from(dsl::col(name)))
}

#[pyfunction]
pub fn lit(item: &PyAny) -> PyResult<PyExpr> {
    if let Ok(true) = item.is_instance_of::<PyBool>() {
        let val = item.extract::<bool>().unwrap();
        Ok(dsl::lit(val).into())
    } else if let Ok(int) = item.downcast::<PyInt>() {
        match int.extract::<i64>() {
            Ok(val) => {
                if val >= 0 && val < i32::MAX as i64 || val <= 0 && val > i32::MIN as i64 {
                    Ok(dsl::lit(val as i32).into())
                } else {
                    Ok(dsl::lit(val).into())
                }
            }
            _ => {
                let val = int.extract::<u64>().unwrap();
                Ok(dsl::lit(val).into())
            }
        }
    } else if let Ok(float) = item.downcast::<PyFloat>() {
        let val = float.extract::<f64>().unwrap();
        Ok(dsl::lit(val).into())
    } else if let Ok(pystr) = item.downcast::<PyString>() {
        Ok(dsl::lit(
            pystr
                .to_str()
                .expect("could not transform Python string to Rust Unicode"),
        )
        .into())
    } else if item.is_none() {
        Ok(dsl::null_lit().into())
    } else {
        Err(PyValueError::new_err(format!(
            "could not convert value {:?} as a Literal",
            item.str()?
        )))
    }
}

#[pyclass]
pub struct PyExpr {
    expr: dsl::Expr,
}

#[pymethods]
impl PyExpr {
    pub fn __add__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::Plus, &self.expr, &other.expr).into())
    }

    pub fn __sub__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::Minus, &self.expr, &other.expr).into())
    }

    pub fn __mul__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::Multiply, &self.expr, &other.expr).into())
    }

    pub fn __floordiv__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::FloorDivide, &self.expr, &other.expr).into())
    }

    pub fn __truediv__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::TrueDivide, &self.expr, &other.expr).into())
    }

    pub fn __mod__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::Modulus, &self.expr, &other.expr).into())
    }

    pub fn __and__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::And, &self.expr, &other.expr).into())
    }

    pub fn __or__(&self, other: &Self) -> PyResult<Self> {
        Ok(dsl::binary_op(dsl::Operator::Or, &self.expr, &other.expr).into())
    }

    pub fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<Self> {
        use dsl::{binary_op, Operator};
        match op {
            CompareOp::Lt => Ok(binary_op(Operator::Lt, &self.expr, &other.expr).into()),
            CompareOp::Le => Ok(binary_op(Operator::LtEq, &self.expr, &other.expr).into()),
            CompareOp::Eq => Ok(binary_op(Operator::Eq, &self.expr, &other.expr).into()),
            CompareOp::Ne => Ok(binary_op(Operator::NotEq, &self.expr, &other.expr).into()),
            CompareOp::Gt => Ok(binary_op(Operator::Gt, &self.expr, &other.expr).into()),
            CompareOp::Ge => Ok(binary_op(Operator::GtEq, &self.expr, &other.expr).into()),
        }
    }
}

impl From<dsl::Expr> for PyExpr {
    fn from(value: dsl::Expr) -> Self {
        PyExpr { expr: value }
    }
}

impl From<PyExpr> for dsl::Expr {
    fn from(item: PyExpr) -> Self {
        item.expr
    }
}
