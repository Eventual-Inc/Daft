#![allow(non_snake_case)]
use std::{
    collections::hash_map::DefaultHasher,
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
    utils::display::display_decimal128,
};
use indexmap::IndexMap;
use pyo3::{
    exceptions::PyValueError,
    intern,
    prelude::*,
    pyclass::CompareOp,
    types::{PyBool, PyBytes, PyFloat, PyInt, PyNone, PyString},
    IntoPyObjectExt,
};
use serde::{Deserialize, Serialize};

use crate::{
    expr::{Expr, WindowExpr},
    visitor::accept,
    ExprRef, LiteralValue, Operator,
};

#[pyfunction]
pub fn unresolved_col(name: &str) -> PyExpr {
    PyExpr::from(crate::unresolved_col(name))
}

#[pyfunction]
pub fn resolved_col(name: &str) -> PyExpr {
    PyExpr::from(crate::resolved_col(name))
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

#[pyfunction(signature = (val, tu, tz=None))]
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
pub fn row_number() -> PyResult<PyExpr> {
    let expr = Expr::WindowFunction(WindowExpr::RowNumber);
    Ok(expr.into())
}

#[pyfunction]
pub fn rank() -> PyResult<PyExpr> {
    let expr = Expr::WindowFunction(WindowExpr::Rank);
    Ok(expr.into())
}

#[pyfunction]
pub fn dense_rank() -> PyResult<PyExpr> {
    let expr = Expr::WindowFunction(WindowExpr::DenseRank);
    Ok(expr.into())
}

#[allow(clippy::too_many_arguments)]
#[pyfunction(signature = (
    years=None,
    months=None,
    days=None,
    hours=None,
    minutes=None,
    seconds=None,
    millis=None,
    nanos=None
))]
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
    literal_value(item).map(Expr::Literal).map(Into::into)
}

pub fn literal_value(item: Bound<PyAny>) -> PyResult<LiteralValue> {
    if item.is_instance_of::<PyBool>() {
        let val = item.extract::<bool>()?;
        Ok(crate::literal_value(val))
    } else if let Ok(int) = item.downcast::<PyInt>() {
        match int.extract::<i64>() {
            Ok(val) => {
                if val >= 0 && val < i32::MAX as i64 || val <= 0 && val > i32::MIN as i64 {
                    Ok(crate::literal_value(val as i32))
                } else {
                    Ok(crate::literal_value(val))
                }
            }
            _ => {
                let val = int.extract::<u64>()?;
                Ok(crate::literal_value(val))
            }
        }
    } else if let Ok(float) = item.downcast::<PyFloat>() {
        let val = float.extract::<f64>()?;
        Ok(crate::literal_value(val))
    } else if let Ok(pystr) = item.downcast::<PyString>() {
        Ok(crate::literal_value(pystr.extract::<String>().expect(
            "could not transform Python string to Rust Unicode",
        )))
    } else if let Ok(pybytes) = item.downcast::<PyBytes>() {
        let bytes = pybytes.as_bytes();
        Ok(crate::literal_value(bytes))
    } else if item.is_none() {
        Ok(LiteralValue::Null)
    } else {
        Ok(crate::literal_value::<PyObject>(item.into()))
    }
}

#[pyfunction]
pub fn list_(items: Vec<PyExpr>) -> PyExpr {
    Expr::List(items.into_iter().map(|item| item.into()).collect()).into()
}

#[allow(clippy::too_many_arguments)]
#[pyfunction(signature = (
    name,
    inner,
    bound_args,
    expressions,
    return_dtype,
    init_args,
    resource_request=None,
    batch_size=None,
    concurrency=None
))]
pub fn udf(
    name: &str,
    inner: PyObject,
    bound_args: PyObject,
    expressions: Vec<PyExpr>,
    return_dtype: PyDataType,
    init_args: PyObject,
    resource_request: Option<ResourceRequest>,
    batch_size: Option<usize>,
    concurrency: Option<usize>,
) -> PyResult<PyExpr> {
    use crate::functions::python::udf;

    if let Some(batch_size) = batch_size {
        if batch_size == 0 {
            return Err(PyValueError::new_err(format!(
                "Error creating UDF: batch size must be positive (got {batch_size})"
            )));
        }
    }

    let expressions_map: Vec<ExprRef> = expressions.into_iter().map(|pyexpr| pyexpr.expr).collect();
    Ok(PyExpr {
        expr: udf(
            name,
            inner.into(),
            bound_args.into(),
            &expressions_map,
            return_dtype.dtype,
            init_args.into(),
            resource_request,
            batch_size,
            concurrency,
        )?
        .into(),
    })
}

/// Initializes all uninitialized UDFs in the expression
#[pyfunction]
pub fn initialize_udfs(expr: PyExpr) -> PyResult<PyExpr> {
    use crate::functions::python::initialize_udfs;
    Ok(initialize_udfs(expr.expr)?.into())
}

/// Get the names of all UDFs in expression
#[pyfunction]
pub fn get_udf_names(expr: PyExpr) -> Vec<String> {
    use crate::functions::python::get_udf_names;
    get_udf_names(&expr.expr)
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

#[derive(FromPyObject)]
pub enum ApproxPercentileInput {
    Single(f64),
    Many(Vec<f64>),
}

impl PyExpr {
    /// converts the pyexpr into a `daft.Expression` python instance
    /// `daft.Expression._from_pyexpr(self)`
    pub fn into_expr_cls(self, py: Python) -> PyResult<PyObject> {
        let daft = py.import("daft")?;
        let expr_cls = daft.getattr("Expression")?;
        let expr = expr_cls.call_method1("_from_pyexpr", (self,))?;
        Ok(expr.unbind())
    }
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

    pub fn count_distinct(&self) -> PyResult<Self> {
        Ok(self.expr.clone().count_distinct().into())
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

    pub fn bool_and(&self) -> PyResult<Self> {
        Ok(self.expr.clone().bool_and().into())
    }

    pub fn bool_or(&self) -> PyResult<Self> {
        Ok(self.expr.clone().bool_or().into())
    }

    pub fn any_value(&self, ignore_nulls: bool) -> PyResult<Self> {
        Ok(self.expr.clone().any_value(ignore_nulls).into())
    }

    pub fn skew(&self) -> PyResult<Self> {
        Ok(self.expr.clone().skew().into())
    }

    pub fn agg_list(&self) -> PyResult<Self> {
        Ok(self.expr.clone().agg_list().into())
    }

    pub fn agg_set(&self) -> PyResult<Self> {
        Ok(self.expr.clone().agg_set().into())
    }

    pub fn agg_concat(&self) -> PyResult<Self> {
        Ok(self.expr.clone().agg_concat().into())
    }

    pub fn __add__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::Plus, self.into(), other.expr.clone()).into())
    }

    pub fn __sub__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::Minus, self.into(), other.expr.clone()).into())
    }

    pub fn __mul__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::Multiply, self.into(), other.expr.clone()).into())
    }

    pub fn __floordiv__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::FloorDivide, self.into(), other.expr.clone()).into())
    }

    pub fn __truediv__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::TrueDivide, self.into(), other.expr.clone()).into())
    }

    pub fn __mod__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::Modulus, self.into(), other.expr.clone()).into())
    }

    pub fn __and__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::And, self.into(), other.expr.clone()).into())
    }

    pub fn __or__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::Or, self.into(), other.expr.clone()).into())
    }

    pub fn __xor__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::Xor, self.into(), other.expr.clone()).into())
    }

    pub fn __lshift__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::ShiftLeft, self.into(), other.expr.clone()).into())
    }

    pub fn __rshift__(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::ShiftRight, self.into(), other.expr.clone()).into())
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

    pub fn eq_null_safe(&self, other: &Self) -> PyResult<Self> {
        Ok(crate::binary_op(Operator::EqNullSafe, self.into(), other.into()).into())
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

    pub fn over(&self, window_spec: &crate::expr::window::WindowSpec) -> PyResult<Self> {
        let window_expr = WindowExpr::try_from(self.expr.clone())?;
        Ok(Self {
            expr: Arc::new(Expr::Over(window_expr, window_spec.clone())),
        })
    }

    #[pyo3(signature = (offset, default=None))]
    pub fn offset(&self, offset: isize, default: Option<&Self>) -> PyResult<Self> {
        let default = default.map(|e| e.expr.clone());
        Ok(Self {
            expr: Arc::new(Expr::WindowFunction(WindowExpr::Offset {
                input: self.expr.clone(),
                offset,
                default,
            })),
        })
    }

    pub fn accept<'py>(&self, visitor: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        accept(&self.clone(), visitor)
    }

    pub fn _eq(&self, other: &Self) -> bool {
        self.expr == other.expr
    }

    pub fn _ne(&self, other: &Self) -> bool {
        self.expr != other.expr
    }

    pub fn _hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.expr.hash(&mut hasher);
        hasher.finish()
    }

    pub fn as_py<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Expr::Literal(lit) = self.expr.as_ref() {
            lit.clone().into_pyobject(py)
        } else {
            Err(PyValueError::new_err(format!(
                "The method .py_any() was called on a non literal value! Got: {}",
                &self.expr
            )))
        }
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

impl<'py> IntoPyObject<'py> for LiteralValue {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        fn div_rem(l: i64, r: i64) -> (i64, i64) {
            (l / r, l % r)
        }

        match self {
            Self::Null => Ok(PyNone::get(py).to_owned().into_any()),
            Self::Boolean(val) => val.into_bound_py_any(py),
            Self::Utf8(val) => val.into_bound_py_any(py),
            Self::Binary(val) => val.into_bound_py_any(py),
            Self::FixedSizeBinary(val, _) => val.into_bound_py_any(py),
            Self::Int8(val) => val.into_bound_py_any(py),
            Self::UInt8(val) => val.into_bound_py_any(py),
            Self::Int16(val) => val.into_bound_py_any(py),
            Self::UInt16(val) => val.into_bound_py_any(py),
            Self::Int32(val) => val.into_bound_py_any(py),
            Self::UInt32(val) => val.into_bound_py_any(py),
            Self::Int64(val) => val.into_bound_py_any(py),
            Self::UInt64(val) => val.into_bound_py_any(py),
            Self::Timestamp(val, time_unit, tz) => {
                let ts = (val as f64) / (time_unit.to_scale_factor() as f64);

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "datetime"))?
                    .call_method1(intern!(py, "fromtimestamp"), (ts, tz))
            }
            Self::Date(val) => py
                .import(intern!(py, "datetime"))?
                .getattr(intern!(py, "date"))?
                .call_method1(intern!(py, "fromtimestamp"), (val,)),
            Self::Time(val, time_unit) => {
                let (h, m, s, us) = match time_unit {
                    TimeUnit::Nanoseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000_000_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000_000_000);
                        let (s, rem) = div_rem(rem, 1_000_000_000);
                        let us = rem / 1_000;
                        (h, m, s, us)
                    }
                    TimeUnit::Microseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000_000);
                        let (s, us) = div_rem(rem, 1_000_000);
                        (h, m, s, us)
                    }
                    TimeUnit::Milliseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000);
                        let (s, ms) = div_rem(rem, 1_000);
                        let us = ms * 1_000;
                        (h, m, s, us)
                    }
                    TimeUnit::Seconds => {
                        let (h, rem) = div_rem(val, 60 * 60);
                        let (m, s) = div_rem(rem, 60);
                        (h, m, s, 0)
                    }
                };

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "time"))?
                    .call1((h, m, s, us))
            }
            Self::Duration(val, time_unit) => {
                let (d, s, us) = match time_unit {
                    TimeUnit::Nanoseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000_000_000);
                        let (s, rem) = div_rem(rem, 1_000_000_000);
                        let us = rem / 1_000;
                        (d, s, us)
                    }
                    TimeUnit::Microseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000_000);
                        let (s, us) = div_rem(rem, 1_000_000);
                        (d, s, us)
                    }
                    TimeUnit::Milliseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000);
                        let (s, ms) = div_rem(rem, 1_000);
                        let us = ms * 1_000;
                        (d, s, us)
                    }
                    TimeUnit::Seconds => {
                        let (d, s) = div_rem(val, 24 * 60 * 60);
                        (d, s, 0)
                    }
                };

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "timedelta"))?
                    .call1((d, s, us))
            }
            Self::Interval(_) => {
                Err(DaftError::NotImplemented("Interval literal to Python".to_string()).into())
            }
            Self::Float64(val) => val.into_bound_py_any(py),
            Self::Decimal(val, p, s) => py
                .import(intern!(py, "decimal"))?
                .getattr(intern!(py, "Decimal"))?
                .call1((display_decimal128(val, p, s),)),
            Self::Series(series) => py
                .import(intern!(py, "daft.series"))?
                .getattr(intern!(py, "Series"))?
                .getattr(intern!(py, "_from_pyseries"))?
                .call1((PySeries { series },)),
            Self::Python(val) => val.0.as_ref().into_bound_py_any(py),
            Self::Struct(entries) => entries
                .into_iter()
                .map(|(f, v)| (f.name, v))
                .collect::<IndexMap<_, _>>()
                .into_bound_py_any(py),
        }
    }
}
