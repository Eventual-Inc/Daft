#![allow(non_snake_case)]
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    str::FromStr,
    sync::Arc,
};

use common_error::DaftError;
use common_hashable_float_wrapper::FloatWrapper;
use common_py_serde::impl_bincode_py_state_serialization;
use common_resource_request::ResourceRequest;
use daft_core::{
    datatypes::{IntervalValue, IntervalValueBuilder},
    prelude::*,
    python::{PyDataType, PyField, PySchema, PySeries, PyTimeUnit},
};
use pyo3::{exceptions::PyValueError, prelude::*, pyclass::CompareOp};
use serde::{Deserialize, Serialize};

use crate::{
    ExprRef, Operator,
    expr::{Expr, VLLMExpr, WindowExpr},
    visitor::accept,
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
pub fn bound_col(index: usize, field: PyField) -> PyExpr {
    crate::bound_col(index, field.field).into()
}

#[pyfunction]
pub fn date_lit(item: i32) -> PyResult<PyExpr> {
    let expr = Expr::Literal(Literal::Date(item));
    Ok(expr.into())
}

#[pyfunction]
pub fn time_lit(item: i64, tu: PyTimeUnit) -> PyResult<PyExpr> {
    let expr = Expr::Literal(Literal::Time(item, tu.timeunit));
    Ok(expr.into())
}

#[pyfunction(signature = (val, tu, tz=None))]
pub fn timestamp_lit(val: i64, tu: PyTimeUnit, tz: Option<String>) -> PyResult<PyExpr> {
    let expr = Expr::Literal(Literal::Timestamp(val, tu.timeunit, tz));
    Ok(expr.into())
}

#[pyfunction]
pub fn duration_lit(val: i64, tu: PyTimeUnit) -> PyResult<PyExpr> {
    let expr = Expr::Literal(Literal::Duration(val, tu.timeunit));
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
    let expr = Expr::Literal(Literal::Interval(iv));
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
    let expr = Expr::Literal(Literal::Decimal(
        v,
        u8::try_from(precision)?,
        i8::try_from(scale)?,
    ));
    Ok(expr.into())
}

#[pyfunction]
pub fn list_lit(series: PySeries) -> PyResult<PyExpr> {
    let expr = Expr::Literal(Literal::List(series.series));
    Ok(expr.into())
}

#[pyfunction]
pub fn lit(item: Bound<PyAny>) -> PyResult<PyExpr> {
    Literal::from_pyobj(&item, None).map(|l| Expr::Literal(l).into())
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
    concurrency=None,
    use_process=None,
    ray_options=None,
))]
pub fn udf(
    name: &str,
    inner: Py<PyAny>,
    bound_args: Py<PyAny>,
    expressions: Vec<PyExpr>,
    return_dtype: PyDataType,
    init_args: Py<PyAny>,
    resource_request: Option<ResourceRequest>,
    batch_size: Option<usize>,
    concurrency: Option<usize>,
    use_process: Option<bool>,
    ray_options: Option<Py<PyAny>>,
) -> PyResult<PyExpr> {
    use crate::functions::python::udf;

    if let Some(batch_size) = batch_size
        && batch_size == 0
    {
        return Err(PyValueError::new_err(format!(
            "Error creating UDF: batch size must be positive (got {batch_size})"
        )));
    }

    let expressions_map: Vec<ExprRef> = expressions.into_iter().map(|pyexpr| pyexpr.expr).collect();

    let concurrency = concurrency
        .map(|c| {
            NonZeroUsize::new(c)
                .ok_or_else(|| PyValueError::new_err("concurrency for udf must be non-zero"))
        })
        .transpose()?;
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
            use_process,
            ray_options.map(|r| r.into()),
        )?
        .into(),
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn row_wise_udf(
    name: &str,
    cls: Py<PyAny>,
    method: Py<PyAny>,
    is_async: bool,
    return_dtype: PyDataType,
    gpus: usize,
    use_process: Option<bool>,
    max_concurrency: Option<usize>,
    max_retries: Option<usize>,
    on_error: Option<String>,
    original_args: Py<PyAny>,
    expr_args: Vec<PyExpr>,
) -> PyResult<PyExpr> {
    let args = expr_args.into_iter().map(|pyexpr| pyexpr.expr).collect();

    // Convert string on_error to OnError enum
    let on_error_enum = on_error
        .as_ref()
        .and_then(|s| crate::functions::python::OnError::from_str(s).ok());

    if on_error.is_some() && on_error_enum.is_none() {
        return Err(PyValueError::new_err(
            "Invalid on_error value. Must be one of: 'raise', 'log', or 'ignore'",
        ));
    }

    let max_concurrency = max_concurrency
        .map(|c| {
            NonZeroUsize::new(c)
                .ok_or_else(|| PyValueError::new_err("max_concurrency for udf must be non-zero"))
        })
        .transpose()?;

    Ok(PyExpr {
        expr: crate::python_udf::row_wise_udf(
            name,
            cls.into(),
            method.into(),
            is_async,
            return_dtype.into(),
            gpus,
            use_process,
            max_concurrency,
            max_retries,
            on_error_enum.unwrap_or_default(),
            original_args.into(),
            args,
        )
        .into(),
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub fn batch_udf(
    name: &str,
    cls: Py<PyAny>,
    method: Py<PyAny>,
    is_async: bool,
    return_dtype: PyDataType,
    gpus: usize,
    use_process: Option<bool>,
    max_concurrency: Option<usize>,
    batch_size: Option<usize>,
    max_retries: Option<usize>,
    on_error: Option<String>,
    original_args: Py<PyAny>,
    expr_args: Vec<PyExpr>,
) -> PyResult<PyExpr> {
    let args = expr_args.into_iter().map(|pyexpr| pyexpr.expr).collect();
    let on_error = on_error
        .and_then(|v| crate::functions::python::OnError::from_str(&v).ok())
        .unwrap_or_default();

    let max_concurrency = max_concurrency
        .map(|c| {
            NonZeroUsize::new(c)
                .ok_or_else(|| PyValueError::new_err("max_concurrency for udf must be non-zero"))
        })
        .transpose()?;

    Ok(PyExpr {
        expr: crate::python_udf::batch_udf(
            name,
            cls.into(),
            method.into(),
            is_async,
            return_dtype.into(),
            gpus,
            use_process,
            max_concurrency,
            batch_size,
            original_args.into(),
            args,
            max_retries,
            on_error,
        )
        .into(),
    })
}

/// Initializes all uninitialized UDFs in the expression
#[pyfunction]
pub fn initialize_udfs(expr: PyExpr) -> PyResult<PyExpr> {
    use crate::functions::python::initialize_udfs;
    Ok(initialize_udfs(expr.expr)?.into())
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
    pub fn into_expr_cls(self, py: Python) -> PyResult<Py<PyAny>> {
        let daft = py.import("daft")?;
        let expr_cls = daft.getattr("Expression")?;
        let expr = expr_cls.call_method1("_from_pyexpr", (self,))?;
        Ok(expr.unbind())
    }
}

#[pymethods]
impl PyExpr {
    pub fn is_column(&self) -> PyResult<bool> {
        Ok(matches!(self.expr.as_ref(), Expr::Column(_)))
    }

    pub fn is_literal(&self) -> PyResult<bool> {
        Ok(matches!(self.expr.as_ref(), Expr::Literal(_)))
    }

    pub fn column_name(&self) -> PyResult<Option<String>> {
        #[allow(deprecated)]
        let name = match self.expr.as_ref() {
            Expr::Column(column) => Some(column.name()),
            _ => None,
        };
        Ok(name)
    }

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

    pub fn product(&self) -> PyResult<Self> {
        Ok(self.expr.clone().product().into())
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
        use crate::{Operator, binary_op};
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
            expr: Arc::new(Expr::Over(window_expr, Arc::new(window_spec.clone()))),
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

    #[allow(clippy::too_many_arguments)]
    pub fn vllm(
        &self,
        model: String,
        concurrency: usize,
        gpus_per_actor: usize,
        do_prefix_routing: bool,
        max_buffer_size: usize,
        min_bucket_size: usize,
        prefix_match_threshold: f64,
        load_balance_threshold: usize,
        batch_size: Option<usize>,
        engine_args: Py<PyAny>,
        generate_args: Py<PyAny>,
    ) -> PyResult<Self> {
        Ok(Expr::VLLM(VLLMExpr {
            input: self.expr.clone(),
            model,
            concurrency,
            gpus_per_actor,
            do_prefix_routing,
            max_buffer_size,
            min_bucket_size,
            prefix_match_threshold: FloatWrapper(prefix_match_threshold),
            load_balance_threshold,
            batch_size,
            engine_args: engine_args.into(),
            generate_args: generate_args.into(),
        })
        .into())
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
