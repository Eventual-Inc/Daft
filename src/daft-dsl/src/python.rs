use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::{functions, optimization, Expr};
use daft_core::{
    count_mode::CountMode,
    datatypes::ImageFormat,
    impl_bincode_py_state_serialization,
    python::{datatype::PyDataType, field::PyField, schema::PySchema},
};

use common_io_config::python::IOConfig as PyIOConfig;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    pyclass::CompareOp,
    types::{PyBool, PyBytes, PyFloat, PyInt, PyString},
    PyTypeInfo,
};

#[pyfunction]
pub fn col(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr::from(crate::col(name)))
}

#[pyfunction]
pub fn lit(item: &PyAny) -> PyResult<PyExpr> {
    if item.is_instance_of::<PyBool>() {
        let val = item.extract::<bool>()?;
        Ok(crate::lit(val).into())
    } else if let Ok(int) = item.downcast::<PyInt>() {
        match int.extract::<i64>() {
            Ok(val) => {
                if val >= 0 && val < i32::MAX as i64 || val <= 0 && val > i32::MIN as i64 {
                    Ok(crate::lit(val as i32).into())
                } else {
                    Ok(crate::lit(val).into())
                }
            }
            _ => {
                let val = int.extract::<u64>()?;
                Ok(crate::lit(val).into())
            }
        }
    } else if let Ok(float) = item.downcast::<PyFloat>() {
        let val = float.extract::<f64>()?;
        Ok(crate::lit(val).into())
    } else if let Ok(pystr) = item.downcast::<PyString>() {
        Ok(crate::lit(
            pystr
                .to_str()
                .expect("could not transform Python string to Rust Unicode"),
        )
        .into())
    } else if let Ok(pybytes) = item.downcast::<PyBytes>() {
        let bytes = pybytes.as_bytes();
        Ok(crate::lit(bytes).into())
    } else if item.is_none() {
        Ok(crate::null_lit().into())
    } else {
        Ok(crate::lit::<PyObject>(item.into()).into())
    }
}

// Create a UDF Expression using:
// * `func` - a Python function that takes as input an ordered list of Python Series to execute the user's UDF.
// * `expressions` - an ordered list of Expressions, each representing computation that will be performed, producing a Series to pass into `func`
// * `return_dtype` - returned column's DataType
#[pyfunction]
pub fn udf(
    py: Python,
    func: &PyAny,
    expressions: Vec<PyExpr>,
    return_dtype: PyDataType,
) -> PyResult<PyExpr> {
    use crate::functions::python::udf;

    // Convert &PyAny values to a GIL-independent reference to Python objects (PyObject) so that we can store them in our Rust Expr enums
    // See: https://pyo3.rs/v0.18.2/types#pyt-and-pyobject
    let func = func.to_object(py);
    let expressions_map: Vec<Expr> = expressions.into_iter().map(|pyexpr| pyexpr.expr).collect();
    Ok(PyExpr {
        expr: udf(func, &expressions_map, return_dtype.dtype)?,
    })
}

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyExpr {
    pub expr: crate::Expr,
}

#[pyfunction]
pub fn eq(expr1: &PyExpr, expr2: &PyExpr) -> PyResult<bool> {
    Ok(expr1.expr == expr2.expr)
}

#[pymethods]
impl PyExpr {
    pub fn _input_mapping(&self) -> PyResult<Option<String>> {
        Ok(self.expr.input_mapping())
    }

    pub fn _required_columns(&self) -> PyResult<HashSet<String>> {
        let mut hs = HashSet::new();
        for name in optimization::get_required_columns(&self.expr) {
            hs.insert(name);
        }
        Ok(hs)
    }

    pub fn _is_column(&self) -> PyResult<bool> {
        Ok(matches!(self.expr, Expr::Column(..)))
    }

    pub fn _replace_column_with_expression(&self, column: &str, new_expr: &Self) -> PyResult<Self> {
        Ok(PyExpr {
            expr: optimization::replace_column_with_expression(&self.expr, column, &new_expr.expr),
        })
    }

    pub fn alias(&self, name: &str) -> PyResult<Self> {
        Ok(self.expr.alias(name).into())
    }

    pub fn cast(&self, dtype: PyDataType) -> PyResult<Self> {
        Ok(self.expr.cast(&dtype.into()).into())
    }

    pub fn if_else(&self, if_true: &Self, if_false: &Self) -> PyResult<Self> {
        Ok(self.expr.if_else(&if_true.expr, &if_false.expr).into())
    }

    pub fn count(&self, mode: CountMode) -> PyResult<Self> {
        Ok(self.expr.count(mode).into())
    }

    pub fn sum(&self) -> PyResult<Self> {
        Ok(self.expr.sum().into())
    }

    pub fn mean(&self) -> PyResult<Self> {
        Ok(self.expr.mean().into())
    }

    pub fn min(&self) -> PyResult<Self> {
        Ok(self.expr.min().into())
    }

    pub fn max(&self) -> PyResult<Self> {
        Ok(self.expr.max().into())
    }

    pub fn agg_list(&self) -> PyResult<Self> {
        Ok(self.expr.agg_list().into())
    }

    pub fn agg_concat(&self) -> PyResult<Self> {
        Ok(self.expr.agg_concat().into())
    }

    pub fn explode(&self) -> PyResult<Self> {
        use functions::list::explode;
        Ok(explode(&self.expr).into())
    }

    pub fn __abs__(&self) -> PyResult<Self> {
        use functions::numeric::abs;
        Ok(abs(&self.expr).into())
    }

    pub fn __add__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Plus, &self.expr, &other.expr).into())
    }

    pub fn __sub__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Minus, &self.expr, &other.expr).into())
    }

    pub fn __mul__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Multiply, &self.expr, &other.expr).into())
    }

    pub fn __floordiv__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::FloorDivide, &self.expr, &other.expr).into())
    }

    pub fn __truediv__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::TrueDivide, &self.expr, &other.expr).into())
    }

    pub fn __mod__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Modulus, &self.expr, &other.expr).into())
    }

    pub fn __and__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::And, &self.expr, &other.expr).into())
    }

    pub fn __or__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Or, &self.expr, &other.expr).into())
    }

    pub fn __xor__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Xor, &self.expr, &other.expr).into())
    }

    pub fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<Self> {
        use crate::{binary_op, Operator};
        match op {
            CompareOp::Lt => Ok(binary_op(Operator::Lt, &self.expr, &other.expr).into()),
            CompareOp::Le => Ok(binary_op(Operator::LtEq, &self.expr, &other.expr).into()),
            CompareOp::Eq => Ok(binary_op(Operator::Eq, &self.expr, &other.expr).into()),
            CompareOp::Ne => Ok(binary_op(Operator::NotEq, &self.expr, &other.expr).into()),
            CompareOp::Gt => Ok(binary_op(Operator::Gt, &self.expr, &other.expr).into()),
            CompareOp::Ge => Ok(binary_op(Operator::GtEq, &self.expr, &other.expr).into()),
        }
    }

    pub fn __invert__(&self) -> PyResult<Self> {
        Ok(self.expr.not().into())
    }

    pub fn is_null(&self) -> PyResult<Self> {
        Ok(self.expr.is_null().into())
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

    pub fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.expr.hash(&mut hasher);
        hasher.finish()
    }

    pub fn is_nan(&self) -> PyResult<Self> {
        use functions::float::is_nan;
        Ok(is_nan(&self.expr).into())
    }

    pub fn dt_date(&self) -> PyResult<Self> {
        use functions::temporal::date;
        Ok(date(&self.expr).into())
    }

    pub fn dt_day(&self) -> PyResult<Self> {
        use functions::temporal::day;
        Ok(day(&self.expr).into())
    }

    pub fn dt_month(&self) -> PyResult<Self> {
        use functions::temporal::month;
        Ok(month(&self.expr).into())
    }

    pub fn dt_year(&self) -> PyResult<Self> {
        use functions::temporal::year;
        Ok(year(&self.expr).into())
    }

    pub fn dt_day_of_week(&self) -> PyResult<Self> {
        use functions::temporal::day_of_week;
        Ok(day_of_week(&self.expr).into())
    }

    pub fn utf8_endswith(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::endswith;
        Ok(endswith(&self.expr, &pattern.expr).into())
    }

    pub fn utf8_startswith(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::startswith;
        Ok(startswith(&self.expr, &pattern.expr).into())
    }

    pub fn utf8_contains(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::contains;
        Ok(contains(&self.expr, &pattern.expr).into())
    }

    pub fn utf8_split(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::split;
        Ok(split(&self.expr, &pattern.expr).into())
    }

    pub fn utf8_length(&self) -> PyResult<Self> {
        use crate::functions::utf8::length;
        Ok(length(&self.expr).into())
    }

    pub fn image_decode(&self) -> PyResult<Self> {
        use crate::functions::image::decode;
        Ok(decode(&self.expr).into())
    }

    pub fn image_encode(&self, image_format: ImageFormat) -> PyResult<Self> {
        use crate::functions::image::encode;
        Ok(encode(&self.expr, image_format).into())
    }

    pub fn image_resize(&self, w: i64, h: i64) -> PyResult<Self> {
        if w < 0 {
            return Err(PyValueError::new_err(format!(
                "width can not be negative: {w}"
            )));
        }
        if h < 0 {
            return Err(PyValueError::new_err(format!(
                "height can not be negative: {h}"
            )));
        }
        use crate::functions::image::resize;
        Ok(resize(&self.expr, w as u32, h as u32).into())
    }

    pub fn image_crop(&self, bbox: &Self) -> PyResult<Self> {
        use crate::functions::image::crop;
        Ok(crop(&self.expr, &bbox.expr).into())
    }

    pub fn list_join(&self, delimiter: &Self) -> PyResult<Self> {
        use crate::functions::list::join;
        Ok(join(&self.expr, &delimiter.expr).into())
    }

    pub fn list_lengths(&self) -> PyResult<Self> {
        use crate::functions::list::lengths;
        Ok(lengths(&self.expr).into())
    }

    pub fn url_download(
        &self,
        max_connections: i64,
        raise_error_on_failure: bool,
        multi_thread: bool,
        config: Option<PyIOConfig>,
    ) -> PyResult<Self> {
        if max_connections <= 0 {
            return Err(PyValueError::new_err(format!(
                "max_connections must be positive and non_zero: {max_connections}"
            )));
        }
        use crate::functions::uri::download;
        Ok(download(
            &self.expr,
            max_connections as usize,
            raise_error_on_failure,
            multi_thread,
            config.map(|c| c.config),
        )
        .into())
    }
}

impl_bincode_py_state_serialization!(PyExpr);

impl From<crate::Expr> for PyExpr {
    fn from(value: crate::Expr) -> Self {
        PyExpr { expr: value }
    }
}

impl From<PyExpr> for crate::Expr {
    fn from(item: PyExpr) -> Self {
        item.expr
    }
}
