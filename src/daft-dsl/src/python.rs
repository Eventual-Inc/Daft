use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use common_error::DaftError;
use daft_core::python::datatype::PyTimeUnit;
use daft_core::python::PySeries;
use serde::{Deserialize, Serialize};

use crate::{functions, optimization, Expr, LiteralValue};
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

    pub fn alias(&self, name: &str) -> PyResult<Self> {
        Ok(self.expr.alias(name).into())
    }

    pub fn cast(&self, dtype: PyDataType) -> PyResult<Self> {
        Ok(self.expr.cast(&dtype.into()).into())
    }

    pub fn ceil(&self) -> PyResult<Self> {
        use functions::numeric::ceil;
        Ok(ceil(&self.expr).into())
    }

    pub fn floor(&self) -> PyResult<Self> {
        use functions::numeric::floor;
        Ok(floor(&self.expr).into())
    }

    pub fn sign(&self) -> PyResult<Self> {
        use functions::numeric::sign;
        Ok(sign(&self.expr).into())
    }

    pub fn round(&self, decimal: i32) -> PyResult<Self> {
        use functions::numeric::round;
        if decimal < 0 {
            return Err(PyValueError::new_err(format!(
                "decimal can not be negative: {decimal}"
            )));
        }
        Ok(round(&self.expr, decimal).into())
    }

    pub fn sin(&self) -> PyResult<Self> {
        use functions::numeric::sin;
        Ok(sin(&self.expr).into())
    }

    pub fn cos(&self) -> PyResult<Self> {
        use functions::numeric::cos;
        Ok(cos(&self.expr).into())
    }

    pub fn tan(&self) -> PyResult<Self> {
        use functions::numeric::tan;
        Ok(tan(&self.expr).into())
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

    pub fn any_value(&self, ignore_nulls: bool) -> PyResult<Self> {
        Ok(self.expr.any_value(ignore_nulls).into())
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

    pub fn not_null(&self) -> PyResult<Self> {
        Ok(self.expr.not_null().into())
    }

    pub fn fill_null(&self, fill_value: &Self) -> PyResult<Self> {
        Ok(self.expr.fill_null(&fill_value.expr).into())
    }

    pub fn is_in(&self, other: &Self) -> PyResult<Self> {
        Ok(self.expr.is_in(&other.expr).into())
    }

    pub fn name(&self) -> PyResult<&str> {
        Ok(self.expr.name()?)
    }

    pub fn to_sql(&self, db_scheme: &str) -> PyResult<Option<String>> {
        Ok(self.expr.to_sql(db_scheme))
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

    pub fn dt_hour(&self) -> PyResult<Self> {
        use functions::temporal::hour;
        Ok(hour(&self.expr).into())
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

    pub fn utf8_match(&self, pattern: &Self) -> PyResult<Self> {
        use crate::functions::utf8::match_;
        Ok(match_(&self.expr, &pattern.expr).into())
    }

    pub fn utf8_split(&self, pattern: &Self, regex: bool) -> PyResult<Self> {
        use crate::functions::utf8::split;
        Ok(split(&self.expr, &pattern.expr, regex).into())
    }

    pub fn utf8_extract(&self, pattern: &Self, index: usize) -> PyResult<Self> {
        use crate::functions::utf8::extract;
        Ok(extract(&self.expr, &pattern.expr, index).into())
    }

    pub fn utf8_extract_all(&self, pattern: &Self, index: usize) -> PyResult<Self> {
        use crate::functions::utf8::extract_all;
        Ok(extract_all(&self.expr, &pattern.expr, index).into())
    }

    pub fn utf8_replace(&self, pattern: &Self, replacement: &Self, regex: bool) -> PyResult<Self> {
        use crate::functions::utf8::replace;
        Ok(replace(&self.expr, &pattern.expr, &replacement.expr, regex).into())
    }

    pub fn utf8_length(&self) -> PyResult<Self> {
        use crate::functions::utf8::length;
        Ok(length(&self.expr).into())
    }

    pub fn utf8_lower(&self) -> PyResult<Self> {
        use crate::functions::utf8::lower;
        Ok(lower(&self.expr).into())
    }

    pub fn utf8_upper(&self) -> PyResult<Self> {
        use crate::functions::utf8::upper;
        Ok(upper(&self.expr).into())
    }

    pub fn utf8_lstrip(&self) -> PyResult<Self> {
        use crate::functions::utf8::lstrip;
        Ok(lstrip(&self.expr).into())
    }

    pub fn utf8_rstrip(&self) -> PyResult<Self> {
        use crate::functions::utf8::rstrip;
        Ok(rstrip(&self.expr).into())
    }

    pub fn utf8_reverse(&self) -> PyResult<Self> {
        use crate::functions::utf8::reverse;
        Ok(reverse(&self.expr).into())
    }

    pub fn utf8_capitalize(&self) -> PyResult<Self> {
        use crate::functions::utf8::capitalize;
        Ok(capitalize(&self.expr).into())
    }

    pub fn utf8_left(&self, count: &Self) -> PyResult<Self> {
        use crate::functions::utf8::left;
        Ok(left(&self.expr, &count.expr).into())
    }

    pub fn utf8_right(&self, count: &Self) -> PyResult<Self> {
        use crate::functions::utf8::right;
        Ok(right(&self.expr, &count.expr).into())
    }

    pub fn utf8_find(&self, substr: &Self) -> PyResult<Self> {
        use crate::functions::utf8::find;
        Ok(find(&self.expr, &substr.expr).into())
    }

    pub fn image_decode(&self, raise_error_on_failure: bool) -> PyResult<Self> {
        use crate::functions::image::decode;
        Ok(decode(&self.expr, raise_error_on_failure).into())
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

    pub fn list_count(&self, mode: CountMode) -> PyResult<Self> {
        use crate::functions::list::count;
        Ok(count(&self.expr, mode).into())
    }

    pub fn list_get(&self, idx: &Self, default: &Self) -> PyResult<Self> {
        use crate::functions::list::get;
        Ok(get(&self.expr, &idx.expr, &default.expr).into())
    }

    pub fn list_sum(&self) -> PyResult<Self> {
        use crate::functions::list::sum;
        Ok(sum(&self.expr).into())
    }

    pub fn list_mean(&self) -> PyResult<Self> {
        use crate::functions::list::mean;
        Ok(mean(&self.expr).into())
    }

    pub fn list_min(&self) -> PyResult<Self> {
        use crate::functions::list::min;
        Ok(min(&self.expr).into())
    }

    pub fn list_max(&self) -> PyResult<Self> {
        use crate::functions::list::max;
        Ok(max(&self.expr).into())
    }

    pub fn struct_get(&self, name: &str) -> PyResult<Self> {
        use crate::functions::struct_::get;
        Ok(get(&self.expr, name).into())
    }

    pub fn partitioning_days(&self) -> PyResult<Self> {
        use crate::functions::partitioning::days;
        Ok(days(self.expr.clone()).into())
    }

    pub fn partitioning_hours(&self) -> PyResult<Self> {
        use crate::functions::partitioning::hours;
        Ok(hours(self.expr.clone()).into())
    }

    pub fn partitioning_months(&self) -> PyResult<Self> {
        use crate::functions::partitioning::months;
        Ok(months(self.expr.clone()).into())
    }

    pub fn partitioning_years(&self) -> PyResult<Self> {
        use crate::functions::partitioning::years;
        Ok(years(self.expr.clone()).into())
    }

    pub fn partitioning_iceberg_bucket(&self, n: i32) -> PyResult<Self> {
        use crate::functions::partitioning::iceberg_bucket;
        Ok(iceberg_bucket(self.expr.clone(), n).into())
    }

    pub fn partitioning_iceberg_truncate(&self, w: i64) -> PyResult<Self> {
        use crate::functions::partitioning::iceberg_truncate;
        Ok(iceberg_truncate(self.expr.clone(), w).into())
    }

    pub fn json_query(&self, _query: &str) -> PyResult<Self> {
        use crate::functions::json::query;
        Ok(query(&self.expr, _query).into())
    }

    pub fn url_download(
        &self,
        max_connections: i64,
        raise_error_on_failure: bool,
        multi_thread: bool,
        config: PyIOConfig,
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
            Some(config.config),
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
