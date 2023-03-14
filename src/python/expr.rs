use super::field::PyField;
use super::{datatype::PyDataType, schema::PySchema};
use crate::dsl::{self, functions};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    pyclass::CompareOp,
    types::{PyBool, PyBytes, PyFloat, PyInt, PyString, PyTuple},
};

#[pyfunction]
pub fn col(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr::from(dsl::col(name)))
}

#[pyfunction]
pub fn lit(item: &PyAny) -> PyResult<PyExpr> {
    if let Ok(true) = item.is_instance_of::<PyBool>() {
        let val = item.extract::<bool>()?;
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
                let val = int.extract::<u64>()?;
                Ok(dsl::lit(val).into())
            }
        }
    } else if let Ok(float) = item.downcast::<PyFloat>() {
        let val = float.extract::<f64>()?;
        Ok(dsl::lit(val).into())
    } else if let Ok(pystr) = item.downcast::<PyString>() {
        Ok(dsl::lit(
            pystr
                .to_str()
                .expect("could not transform Python string to Rust Unicode"),
        )
        .into())
    } else if let Ok(pybytes) = item.downcast::<PyBytes>() {
        let bytes = pybytes.as_bytes();
        Ok(dsl::lit(bytes).into())
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
#[derive(Clone)]
pub struct PyExpr {
    pub expr: dsl::Expr,
}

#[pymethods]
impl PyExpr {
    #[new]
    #[args(args = "*")]
    fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            0 => Ok(dsl::null_lit().into()),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new PyExpr, got : {}",
                args.len()
            ))),
        }
    }

    pub fn alias(&self, name: &str) -> PyResult<Self> {
        Ok(self.expr.alias(name).into())
    }

    pub fn cast(&self, dtype: PyDataType) -> PyResult<Self> {
        Ok(self.expr.cast(&dtype.into()).into())
    }

    pub fn sum(&self) -> PyResult<Self> {
        Ok(self.expr.sum().into())
    }

    pub fn __abs__(&self) -> PyResult<Self> {
        use functions::numeric::abs;
        Ok(abs(&self.expr).into())
    }

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

    pub fn name(&self) -> PyResult<&str> {
        Ok(self.expr.name()?)
    }

    pub fn to_field(&self, schema: &PySchema) -> PyResult<PyField> {
        Ok(self.expr.to_field(&schema.schema)?.into())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.expr))
    }

    pub fn __setstate__(&mut self, py: Python, state: PyObject) -> PyResult<()> {
        match state.extract::<&PyBytes>(py) {
            Ok(s) => {
                self.expr = bincode::deserialize(s.as_bytes()).unwrap();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn __getstate__(&self, py: Python) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &bincode::serialize(&self.expr).unwrap()).to_object(py))
    }

    pub fn utf8_endswith(&self, pattern: &Self) -> PyResult<Self> {
        use dsl::functions::utf8::endswith;
        Ok(endswith(&self.expr, &pattern.expr).into())
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
