#![allow(non_snake_case)]
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::DaftError;
use common_py_serde::impl_bincode_py_state_serialization;
use common_resource_request::ResourceRequest;
use daft_core::{
    datatypes::{IntervalValue, IntervalValueBuilder},
    prelude::*,
    python::{PyDataType, PyField, PySchema, PySeries, PyTimeUnit},
};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    pyclass::CompareOp,
    types::{PyBool, PyBytes, PyFloat, PyInt, PyString},
};
use serde::{Deserialize, Serialize};

use crate::{Expr, ExprRef, LiteralValue};

#[pyfunction]
pub fn col(name: &str) -> PyResult<PyExpr> {
    Ok(PyExpr::from(crate::col(name)))
}

#[pyfunction]
pub fn date_lit(item: i32) -> PyResult<PyExpr> {
    let expr = Expr::Literal(LiteralValue::Date(item));
    Ok(expr.into())
}

#[pyfunction]
pub fn time_lit(item: i64, tu: PyTimeUnit) -> PyResult<PyExpr> {
    let expr = Expr::Literal(LiteralValue::Time(item, tu.timeunit));
    Ok(expr.into())
}

#[pyfunction]
pub fn timestamp_lit(val: i64, tu: PyTimeUnit, tz: Option<String>) -> PyResult<PyExpr> {
    let expr = Expr::Literal(LiteralValue::Timestamp(val, tu.timeunit, tz));
    Ok(expr.into())
}

#[pyfunction]
pub fn duration_lit(val: i64, tu: PyTimeUnit) -> PyResult<PyExpr> {
    let expr = Expr::Literal(LiteralValue::Duration(val, tu.timeunit));
    Ok(expr.into())
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn interval_lit(
    years: Option<i32>,
    months: Option<i32>,
    days: Option<i32>,
    hours: Option<i32>,
    minutes: Option<i32>,
    seconds: Option<i32>,
    millis: Option<i32>,
    nanos: Option<i64>,
) -> PyResult<PyExpr> {
    let opts = IntervalValueBuilder {
        years,
        months,
        days,
        hours,
        minutes,
        seconds,
        milliseconds: millis,
        nanoseconds: nanos,
    };
    let iv = IntervalValue::try_new(opts)?;
    let expr = Expr::Literal(LiteralValue::Interval(iv));
    Ok(expr.into())
}

fn decimal_from_digits(digits: Vec<u8>, exp: i32) -> Option<(i128, usize)> {
    const MAX_ABS_DEC: i128 = 10_i128.pow(38) - 1;
    let mut v = 0_i128;
    for (i, d) in digits.into_iter().map(i128::from).enumerate() {
        if i < 38 {
            v = v * 10 + d;
        } else {
            v = v.checked_mul(10).and_then(|v| v.checked_add(d))?;
        }
    }
    // We only support non-negative scales, and therefore non-positive exponents.
    let scale = if exp > 0 {
        // Decimal may be in a non-canonical representation, try to fix it first.
        v = 10_i128
            .checked_pow(exp as u32)
            .and_then(|factor| v.checked_mul(factor))?;
        0
    } else {
        (-exp) as usize
    };
    if v <= MAX_ABS_DEC {
        Some((v, scale))
    } else {
        None
    }
}

#[pyfunction]
pub fn decimal_lit(sign: bool, digits: Vec<u8>, exp: i32) -> PyResult<PyExpr> {
    let num_digits = digits.len();
    let (mut v, scale) = decimal_from_digits(digits, exp).ok_or_else(|| {
        DaftError::ValueError("Decimal is too large to fit into Decimal128 type.".to_string())
    })?;
    let precision = if exp < 0 {
        std::cmp::max(num_digits, (-exp) as usize)
    } else {
        num_digits + (exp as usize)
    };
    if sign {
        v = -v;
    }
    let expr = Expr::Literal(LiteralValue::Decimal(
        v,
        u8::try_from(precision)?,
        i8::try_from(scale)?,
    ));
    Ok(expr.into())
}

#[pyfunction]
pub fn series_lit(series: PySeries) -> PyResult<PyExpr> {
    let expr = Expr::Literal(LiteralValue::Series(series.series));
    Ok(expr.into())
}

#[pyfunction]
pub fn lit(item: Bound<PyAny>) -> PyResult<PyExpr> {
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
                .extract::<String>()
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
pub fn stateless_udf(
    name: &str,
    partial_stateless_udf: PyObject,
    expressions: Vec<PyExpr>,
    return_dtype: PyDataType,
    resource_request: Option<ResourceRequest>,
    batch_size: Option<usize>,
) -> PyResult<PyExpr> {
    use crate::functions::python::stateless_udf;

    if let Some(batch_size) = batch_size {
        if batch_size == 0 {
            return Err(PyValueError::new_err(format!(
                "Error creating UDF: batch size must be positive (got {batch_size})"
            )));
        }
    }

    let expressions_map: Vec<ExprRef> = expressions.into_iter().map(|pyexpr| pyexpr.expr).collect();
    Ok(PyExpr {
        expr: stateless_udf(
            name,
            partial_stateless_udf.into(),
            &expressions_map,
            return_dtype.dtype,
            resource_request,
            batch_size,
        )?
        .into(),
    })
}

// Create a UDF Expression using:
// * `cls` - a Python class that has an __init__, and where __call__ takes as input an ordered list of Python Series to execute the user's UDF.
// * `expressions` - an ordered list of Expressions, each representing computation that will be performed, producing a Series to pass into `func`
// * `return_dtype` - returned column's DataType
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn stateful_udf(
    name: &str,
    partial_stateful_udf: PyObject,
    expressions: Vec<PyExpr>,
    return_dtype: PyDataType,
    resource_request: Option<ResourceRequest>,
    init_args: Option<PyObject>,
    batch_size: Option<usize>,
    concurrency: Option<usize>,
) -> PyResult<PyExpr> {
    use crate::functions::python::stateful_udf;

    if let Some(batch_size) = batch_size {
        if batch_size == 0 {
            return Err(PyValueError::new_err(format!(
                "Error creating UDF: batch size must be positive (got {batch_size})"
            )));
        }
    }

    let expressions_map: Vec<ExprRef> = expressions.into_iter().map(|pyexpr| pyexpr.expr).collect();
    let init_args = init_args.map(|args| args.into());
    Ok(PyExpr {
        expr: stateful_udf(
            name,
            partial_stateful_udf.into(),
            &expressions_map,
            return_dtype.dtype,
            resource_request,
            init_args,
            batch_size,
            concurrency,
        )?
        .into(),
    })
}

/// Extracts the `class PartialStatefulUDF` Python objects that are in the specified expression tree
#[pyfunction]
pub fn extract_partial_stateful_udf_py(
    expr: PyExpr,
) -> HashMap<String, (Py<PyAny>, Option<Py<PyAny>>)> {
    use crate::functions::python::extract_partial_stateful_udf_py;
    extract_partial_stateful_udf_py(expr.expr)
}

/// Binds the StatefulPythonUDFs in a given expression to any corresponding initialized Python callables in the provided map
#[pyfunction]
pub fn bind_stateful_udfs(
    expr: PyExpr,
    initialized_funcs: HashMap<String, Py<PyAny>>,
) -> PyResult<PyExpr> {
    use crate::functions::python::bind_stateful_udfs;
    Ok(bind_stateful_udfs(expr.expr, &initialized_funcs).map(PyExpr::from)?)
}

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyExpr {
    pub expr: crate::ExprRef,
}

#[pyfunction]
pub fn eq(expr1: &PyExpr, expr2: &PyExpr) -> PyResult<bool> {
    Ok(expr1.expr == expr2.expr)
}

#[pyfunction]
pub fn check_column_name_validity(name: &str, schema: &PySchema) -> PyResult<()> {
    Ok(crate::check_column_name_validity(name, &schema.schema)?)
}

#[derive(FromPyObject)]
pub enum ApproxPercentileInput {
    Single(f64),
    Many(Vec<f64>),
}

#[pymethods]
impl PyExpr {
    pub fn _input_mapping(&self) -> PyResult<Option<String>> {
        Ok(self.expr.input_mapping())
    }

    pub fn alias(&self, name: &str) -> PyResult<Self> {
        Ok(self.expr.clone().alias(name).into())
    }

    pub fn cast(&self, dtype: PyDataType) -> PyResult<Self> {
        Ok(self.expr.clone().cast(&dtype.into()).into())
    }

    pub fn if_else(&self, if_true: &Self, if_false: &Self) -> PyResult<Self> {
        Ok(self
            .expr
            .clone()
            .if_else(if_true.expr.clone(), if_false.expr.clone())
            .into())
    }

    pub fn count(&self, mode: CountMode) -> PyResult<Self> {
        Ok(self.expr.clone().count(mode).into())
    }

    pub fn sum(&self) -> PyResult<Self> {
        Ok(self.expr.clone().sum().into())
    }

    pub fn approx_count_distinct(&self) -> PyResult<Self> {
        Ok(self.expr.clone().approx_count_distinct().into())
    }

    pub fn approx_percentiles(&self, percentiles: ApproxPercentileInput) -> PyResult<Self> {
        let (percentiles, list_output) = match percentiles {
            ApproxPercentileInput::Single(p) => (vec![p], false),
            ApproxPercentileInput::Many(p) => (p, true),
        };

        for &p in &percentiles {
            if !(0. ..=1.).contains(&p) {
                return Err(PyValueError::new_err(format!(
                    "Provided percentile must be between 0 and 1: {}",
                    p
                )));
            }
        }

        Ok(self
            .expr
            .clone()
            .approx_percentiles(percentiles.as_slice(), list_output)
            .into())
    }

    pub fn mean(&self) -> PyResult<Self> {
        Ok(self.expr.clone().mean().into())
    }

    pub fn stddev(&self) -> PyResult<Self> {
        Ok(self.expr.clone().stddev().into())
    }

    pub fn min(&self) -> PyResult<Self> {
        Ok(self.expr.clone().min().into())
    }

    pub fn max(&self) -> PyResult<Self> {
        Ok(self.expr.clone().max().into())
    }

    pub fn any_value(&self, ignore_nulls: bool) -> PyResult<Self> {
        Ok(self.expr.clone().any_value(ignore_nulls).into())
    }

    pub fn agg_list(&self) -> PyResult<Self> {
        Ok(self.expr.clone().agg_list().into())
    }

    pub fn agg_concat(&self) -> PyResult<Self> {
        Ok(self.expr.clone().agg_concat().into())
    }

    pub fn __add__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Plus, self.into(), other.expr.clone()).into())
    }
    pub fn __sub__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Minus, self.into(), other.expr.clone()).into())
    }
    pub fn __mul__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Multiply, self.into(), other.expr.clone()).into())
    }
    pub fn __floordiv__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(
            crate::Operator::FloorDivide,
            self.into(),
            other.expr.clone(),
        )
        .into())
    }

    pub fn __truediv__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::TrueDivide, self.into(), other.expr.clone()).into())
    }

    pub fn __mod__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Modulus, self.into(), other.expr.clone()).into())
    }

    pub fn __and__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::And, self.into(), other.expr.clone()).into())
    }

    pub fn __or__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Or, self.into(), other.expr.clone()).into())
    }

    pub fn __xor__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::Xor, self.into(), other.expr.clone()).into())
    }

    pub fn __lshift__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::ShiftLeft, self.into(), other.expr.clone()).into())
    }

    pub fn __rshift__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(crate::Operator::ShiftRight, self.into(), other.expr.clone()).into())
    }

    pub fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<Self> {
        use crate::{binary_op, Operator};
        match op {
            CompareOp::Lt => Ok(binary_op(Operator::Lt, self.into(), other.into()).into()),
            CompareOp::Le => Ok(binary_op(Operator::LtEq, self.into(), other.into()).into()),
            CompareOp::Eq => Ok(binary_op(Operator::Eq, self.into(), other.into()).into()),
            CompareOp::Ne => Ok(binary_op(Operator::NotEq, self.into(), other.into()).into()),
            CompareOp::Gt => Ok(binary_op(Operator::Gt, self.into(), other.into()).into()),
            CompareOp::Ge => Ok(binary_op(Operator::GtEq, self.into(), other.into()).into()),
        }
    }

    pub fn __invert__(&self) -> PyResult<Self> {
        Ok(self.expr.clone().not().into())
    }

    pub fn is_null(&self) -> PyResult<Self> {
        Ok(self.expr.clone().is_null().into())
    }

    pub fn not_null(&self) -> PyResult<Self> {
        Ok(self.expr.clone().not_null().into())
    }

    pub fn fill_null(&self, fill_value: &Self) -> PyResult<Self> {
        Ok(self.expr.clone().fill_null(fill_value.expr.clone()).into())
    }

    pub fn is_in(&self, other: Vec<Self>) -> PyResult<Self> {
        let other = other.into_iter().map(|e| e.into()).collect();

        Ok(self.expr.clone().is_in(other).into())
    }

    pub fn between(&self, lower: &Self, upper: &Self) -> PyResult<Self> {
        Ok(self
            .expr
            .clone()
            .between(lower.expr.clone(), upper.expr.clone())
            .into())
    }

    pub fn name(&self) -> PyResult<&str> {
        Ok(self.expr.name())
    }

    pub fn to_sql(&self) -> PyResult<Option<String>> {
        Ok(self.expr.to_sql())
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

    pub fn struct_get(&self, name: &str) -> PyResult<Self> {
        use crate::functions::struct_::get;
        Ok(get(self.into(), name).into())
    }

    pub fn map_get(&self, key: &Self) -> PyResult<Self> {
        use crate::functions::map::get;
        Ok(get(self.into(), key.into()).into())
    }

    pub fn partitioning_days(&self) -> PyResult<Self> {
        use crate::functions::partitioning::days;
        Ok(days(self.into()).into())
    }

    pub fn partitioning_hours(&self) -> PyResult<Self> {
        use crate::functions::partitioning::hours;
        Ok(hours(self.into()).into())
    }

    pub fn partitioning_months(&self) -> PyResult<Self> {
        use crate::functions::partitioning::months;
        Ok(months(self.into()).into())
    }

    pub fn partitioning_years(&self) -> PyResult<Self> {
        use crate::functions::partitioning::years;
        Ok(years(self.into()).into())
    }

    pub fn partitioning_iceberg_bucket(&self, n: i32) -> PyResult<Self> {
        use crate::functions::partitioning::iceberg_bucket;
        Ok(iceberg_bucket(self.into(), n).into())
    }

    pub fn partitioning_iceberg_truncate(&self, w: i64) -> PyResult<Self> {
        use crate::functions::partitioning::iceberg_truncate;
        Ok(iceberg_truncate(self.into(), w).into())
    }
}

impl_bincode_py_state_serialization!(PyExpr);

impl From<ExprRef> for PyExpr {
    fn from(value: crate::ExprRef) -> Self {
        Self { expr: value }
    }
}

impl From<Expr> for PyExpr {
    fn from(value: crate::Expr) -> Self {
        Self {
            expr: Arc::new(value),
        }
    }
}

impl From<PyExpr> for crate::ExprRef {
    fn from(item: PyExpr) -> Self {
        item.expr
    }
}

impl From<&PyExpr> for crate::ExprRef {
    fn from(item: &PyExpr) -> Self {
        item.expr.clone()
    }
}
