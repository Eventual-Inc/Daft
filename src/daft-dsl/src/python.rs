use std::collections::hash_map::DefaultHasher;

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use common_error::DaftError;
use daft_core::array::ops::Utf8NormalizeOptions;
use daft_core::python::datatype::PyTimeUnit;
use daft_core::python::PySeries;
use serde::{Deserialize, Serialize};

use crate::{functions, Expr, ExprRef, LiteralValue};
use daft_core::{
    count_mode::CountMode,
    datatypes::{ImageFormat, ImageMode},
    impl_bincode_py_state_serialization,
    python::{datatype::PyDataType, field::PyField, schema::PySchema},
};

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
    let expressions_map: Vec<ExprRef> = expressions.into_iter().map(|pyexpr| pyexpr.expr).collect();
    Ok(PyExpr {
        expr: udf(func, &expressions_map, return_dtype.dtype)?.into(),
    })
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
pub fn resolve_expr(expr: &PyExpr, schema: &PySchema) -> PyResult<(PyExpr, PyField)> {
    let (resolved_expr, field) = crate::resolve_expr(expr.expr.clone(), &schema.schema)?;
    Ok((resolved_expr.into(), field.into()))
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

    pub fn ceil(&self) -> PyResult<Self> {
        use functions::numeric::ceil;
        Ok(ceil(self.into()).into())
    }

    pub fn floor(&self) -> PyResult<Self> {
        use functions::numeric::floor;
        Ok(floor(self.into()).into())
    }

    pub fn sign(&self) -> PyResult<Self> {
        use functions::numeric::sign;
        Ok(sign(self.into()).into())
    }

    pub fn round(&self, decimal: i32) -> PyResult<Self> {
        use functions::numeric::round;
        if decimal < 0 {
            return Err(PyValueError::new_err(format!(
                "decimal can not be negative: {decimal}"
            )));
        }
        Ok(round(self.into(), decimal).into())
    }

    pub fn sqrt(&self) -> PyResult<Self> {
        use functions::numeric::sqrt;
        Ok(sqrt(self.into()).into())
    }

    pub fn sin(&self) -> PyResult<Self> {
        use functions::numeric::sin;
        Ok(sin(self.into()).into())
    }

    pub fn cos(&self) -> PyResult<Self> {
        use functions::numeric::cos;
        Ok(cos(self.into()).into())
    }

    pub fn tan(&self) -> PyResult<Self> {
        use functions::numeric::tan;
        Ok(tan(self.into()).into())
    }

    pub fn cot(&self) -> PyResult<Self> {
        use functions::numeric::cot;
        Ok(cot(self.into()).into())
    }

    pub fn arcsin(&self) -> PyResult<Self> {
        use functions::numeric::arcsin;
        Ok(arcsin(self.into()).into())
    }

    pub fn arccos(&self) -> PyResult<Self> {
        use functions::numeric::arccos;
        Ok(arccos(self.into()).into())
    }

    pub fn arctan(&self) -> PyResult<Self> {
        use functions::numeric::arctan;
        Ok(arctan(self.into()).into())
    }

    pub fn arctan2(&self, other: &Self) -> PyResult<Self> {
        use functions::numeric::arctan2;
        Ok(arctan2(self.into(), other.expr.clone()).into())
    }

    pub fn radians(&self) -> PyResult<Self> {
        use functions::numeric::radians;
        Ok(radians(self.into()).into())
    }

    pub fn degrees(&self) -> PyResult<Self> {
        use functions::numeric::degrees;
        Ok(degrees(self.into()).into())
    }

    pub fn arctanh(&self) -> PyResult<Self> {
        use functions::numeric::arctanh;
        Ok(arctanh(self.into()).into())
    }

    pub fn arccosh(&self) -> PyResult<Self> {
        use functions::numeric::arccosh;
        Ok(arccosh(self.into()).into())
    }

    pub fn arcsinh(&self) -> PyResult<Self> {
        use functions::numeric::arcsinh;
        Ok(arcsinh(self.into()).into())
    }

    pub fn log2(&self) -> PyResult<Self> {
        use functions::numeric::log2;
        Ok(log2(self.into()).into())
    }

    pub fn log10(&self) -> PyResult<Self> {
        use functions::numeric::log10;
        Ok(log10(self.into()).into())
    }

    pub fn log(&self, base: f64) -> PyResult<Self> {
        use functions::numeric::log;
        Ok(log(self.into(), base).into())
    }

    pub fn ln(&self) -> PyResult<Self> {
        use functions::numeric::ln;
        Ok(ln(self.into()).into())
    }

    pub fn exp(&self) -> PyResult<Self> {
        use functions::numeric::exp;
        Ok(exp(self.into()).into())
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

    pub fn approx_percentiles(&self, percentiles: ApproxPercentileInput) -> PyResult<Self> {
        let (percentiles, list_output) = match percentiles {
            ApproxPercentileInput::Single(p) => (vec![p], false),
            ApproxPercentileInput::Many(p) => (p, true),
        };

        for &p in percentiles.iter() {
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

    pub fn explode(&self) -> PyResult<Self> {
        use functions::list::explode;
        Ok(explode(self.into()).into())
    }

    pub fn __abs__(&self) -> PyResult<Self> {
        use functions::numeric::abs;
        Ok(abs(self.into()).into())
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

    pub fn is_in(&self, other: &Self) -> PyResult<Self> {
        Ok(self.expr.clone().is_in(other.expr.clone()).into())
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

    pub fn is_nan(&self) -> PyResult<Self> {
        use functions::float::is_nan;
        Ok(is_nan(self.into()).into())
    }

    pub fn is_inf(&self) -> PyResult<Self> {
        use functions::float::is_inf;
        Ok(is_inf(self.into()).into())
    }

    pub fn not_nan(&self) -> PyResult<Self> {
        use functions::float::not_nan;
        Ok(not_nan(self.into()).into())
    }

    pub fn fill_nan(&self, fill_value: &Self) -> PyResult<Self> {
        use functions::float::fill_nan;
        Ok(fill_nan(self.into(), fill_value.expr.clone()).into())
    }

    pub fn dt_date(&self) -> PyResult<Self> {
        use functions::temporal::date;
        Ok(date(self.into()).into())
    }

    pub fn dt_day(&self) -> PyResult<Self> {
        use functions::temporal::day;
        Ok(day(self.into()).into())
    }

    pub fn dt_hour(&self) -> PyResult<Self> {
        use functions::temporal::hour;
        Ok(hour(self.into()).into())
    }

    pub fn dt_minute(&self) -> PyResult<Self> {
        use functions::temporal::minute;
        Ok(minute(self.into()).into())
    }

    pub fn dt_second(&self) -> PyResult<Self> {
        use functions::temporal::second;
        Ok(second(self.into()).into())
    }

    pub fn dt_time(&self) -> PyResult<Self> {
        use functions::temporal::time;
        Ok(time(self.into()).into())
    }

    pub fn dt_month(&self) -> PyResult<Self> {
        use functions::temporal::month;
        Ok(month(self.into()).into())
    }

    pub fn dt_year(&self) -> PyResult<Self> {
        use functions::temporal::year;
        Ok(year(self.into()).into())
    }

    pub fn dt_day_of_week(&self) -> PyResult<Self> {
        use functions::temporal::day_of_week;
        Ok(day_of_week(self.into()).into())
    }

    pub fn dt_truncate(&self, interval: &str, relative_to: &Self) -> PyResult<Self> {
        use functions::temporal::truncate;
        Ok(truncate(self.into(), interval, relative_to.expr.clone()).into())
    }

    pub fn utf8_endswith(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::endswith;
        Ok(endswith(self.into(), pattern.expr.clone()).into())
    }

    pub fn utf8_startswith(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::startswith;
        Ok(startswith(self.into(), pattern.expr.clone()).into())
    }

    pub fn utf8_contains(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::contains;
        Ok(contains(self.into(), pattern.expr.clone()).into())
    }

    pub fn utf8_match(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::match_;
        Ok(match_(self.into(), pattern.expr.clone()).into())
    }

    pub fn utf8_split(&self, pattern: &Self, regex: bool) -> PyResult<Self> {
        use crate::functions::utf8::split;
        Ok(split(self.into(), pattern.expr.clone(), regex).into())
    }

    pub fn utf8_extract(&self, pattern: &Self, index: usize) -> PyResult<Self> {
        use crate::functions::utf8::extract;
        Ok(extract(self.into(), pattern.expr.clone(), index).into())
    }

    pub fn utf8_extract_all(&self, pattern: &Self, index: usize) -> PyResult<Self> {
        use crate::functions::utf8::extract_all;
        Ok(extract_all(self.into(), pattern.expr.clone(), index).into())
    }

    pub fn utf8_replace(&self, pattern: &Self, replacement: &Self, regex: bool) -> PyResult<Self> {
        use crate::functions::utf8::replace;
        Ok(replace(
            self.into(),
            pattern.expr.clone(),
            replacement.expr.clone(),
            regex,
        )
        .into())
    }

    pub fn utf8_length(&self) -> PyResult<Self> {
        use crate::functions::utf8::length;
        Ok(length(self.into()).into())
    }

    pub fn utf8_lower(&self) -> PyResult<Self> {
        use crate::functions::utf8::lower;
        Ok(lower(self.into()).into())
    }

    pub fn utf8_upper(&self) -> PyResult<Self> {
        use crate::functions::utf8::upper;
        Ok(upper(self.into()).into())
    }

    pub fn utf8_lstrip(&self) -> PyResult<Self> {
        use crate::functions::utf8::lstrip;
        Ok(lstrip(self.into()).into())
    }

    pub fn utf8_rstrip(&self) -> PyResult<Self> {
        use crate::functions::utf8::rstrip;
        Ok(rstrip(self.into()).into())
    }

    pub fn utf8_reverse(&self) -> PyResult<Self> {
        use crate::functions::utf8::reverse;
        Ok(reverse(self.into()).into())
    }

    pub fn utf8_capitalize(&self) -> PyResult<Self> {
        use crate::functions::utf8::capitalize;
        Ok(capitalize(self.into()).into())
    }

    pub fn utf8_left(&self, count: &Self) -> PyResult<Self> {
        use crate::functions::utf8::left;
        Ok(left(self.into(), count.into()).into())
    }

    pub fn utf8_right(&self, count: &Self) -> PyResult<Self> {
        use crate::functions::utf8::right;
        Ok(right(self.into(), count.into()).into())
    }

    pub fn utf8_find(&self, substr: &Self) -> PyResult<Self> {
        use crate::functions::utf8::find;
        Ok(find(self.into(), substr.into()).into())
    }

    pub fn utf8_rpad(&self, length: &Self, pad: &Self) -> PyResult<Self> {
        use crate::functions::utf8::rpad;
        Ok(rpad(self.into(), length.into(), pad.into()).into())
    }

    pub fn utf8_lpad(&self, length: &Self, pad: &Self) -> PyResult<Self> {
        use crate::functions::utf8::lpad;
        Ok(lpad(self.into(), length.into(), pad.into()).into())
    }

    pub fn utf8_repeat(&self, n: &Self) -> PyResult<Self> {
        use crate::functions::utf8::repeat;
        Ok(repeat(self.into(), n.into()).into())
    }

    pub fn utf8_like(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::like;
        Ok(like(self.into(), pattern.into()).into())
    }

    pub fn utf8_ilike(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::ilike;
        Ok(ilike(self.into(), pattern.into()).into())
    }

    pub fn utf8_substr(&self, start: &Self, length: &Self) -> PyResult<Self> {
        use crate::functions::utf8::substr;
        Ok(substr(self.into(), start.into(), length.into()).into())
    }

    pub fn utf8_to_date(&self, format: &str) -> PyResult<Self> {
        use crate::functions::utf8::to_date;
        Ok(to_date(self.into(), format).into())
    }

    pub fn utf8_to_datetime(&self, format: &str, timezone: Option<&str>) -> PyResult<Self> {
        use crate::functions::utf8::to_datetime;
        Ok(to_datetime(self.into(), format, timezone).into())
    }

    pub fn utf8_normalize(
        &self,
        remove_punct: bool,
        lowercase: bool,
        nfd_unicode: bool,
        white_space: bool,
    ) -> PyResult<Self> {
        use crate::functions::utf8::normalize;
        let opts = Utf8NormalizeOptions {
            remove_punct,
            lowercase,
            nfd_unicode,
            white_space,
        };

        Ok(normalize(self.into(), opts).into())
    }

    pub fn image_decode(
        &self,
        raise_error_on_failure: bool,
        mode: Option<ImageMode>,
    ) -> PyResult<Self> {
        use crate::functions::image::decode;
        Ok(decode(self.into(), raise_error_on_failure, mode).into())
    }

    pub fn image_encode(&self, image_format: ImageFormat) -> PyResult<Self> {
        use crate::functions::image::encode;
        Ok(encode(self.into(), image_format).into())
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
        Ok(resize(self.into(), w as u32, h as u32).into())
    }

    pub fn image_crop(&self, bbox: &Self) -> PyResult<Self> {
        use crate::functions::image::crop;
        Ok(crop(self.into(), bbox.into()).into())
    }

    pub fn image_to_mode(&self, mode: ImageMode) -> PyResult<Self> {
        use crate::functions::image::to_mode;
        Ok(to_mode(self.into(), mode).into())
    }

    pub fn list_join(&self, delimiter: &Self) -> PyResult<Self> {
        use crate::functions::list::join;
        Ok(join(self.into(), delimiter.into()).into())
    }

    pub fn list_count(&self, mode: CountMode) -> PyResult<Self> {
        use crate::functions::list::count;
        Ok(count(self.into(), mode).into())
    }

    pub fn list_get(&self, idx: &Self, default: &Self) -> PyResult<Self> {
        use crate::functions::list::get;
        Ok(get(self.into(), idx.into(), default.into()).into())
    }

    pub fn list_sum(&self) -> PyResult<Self> {
        use crate::functions::list::sum;
        Ok(sum(self.into()).into())
    }

    pub fn list_mean(&self) -> PyResult<Self> {
        use crate::functions::list::mean;
        Ok(mean(self.into()).into())
    }

    pub fn list_min(&self) -> PyResult<Self> {
        use crate::functions::list::min;
        Ok(min(self.into()).into())
    }

    pub fn list_max(&self) -> PyResult<Self> {
        use crate::functions::list::max;
        Ok(max(self.into()).into())
    }

    pub fn list_slice(&self, start: &Self, end: &Self) -> PyResult<Self> {
        use crate::functions::list::slice;
        Ok(slice(self.into(), start.into(), end.into()).into())
    }

    pub fn list_chunk(&self, size: usize) -> PyResult<Self> {
        use crate::functions::list::chunk;
        Ok(chunk(self.into(), size).into())
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

    pub fn json_query(&self, _query: &str) -> PyResult<Self> {
        use crate::functions::json::query;
        Ok(query(self.into(), _query).into())
    }
}

impl_bincode_py_state_serialization!(PyExpr);

impl From<ExprRef> for PyExpr {
    fn from(value: crate::ExprRef) -> Self {
        PyExpr { expr: value }
    }
}

impl From<Expr> for PyExpr {
    fn from(value: crate::Expr) -> Self {
        PyExpr {
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
