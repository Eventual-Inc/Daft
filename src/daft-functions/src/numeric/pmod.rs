use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{Float32Array, Float64Array, InferDataType, Int64Array, UInt64Array},
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Pmod;

#[derive(FunctionArgs)]
struct PmodArgs<T> {
    a: T,
    b: T,
}

#[typetag::serde]
impl ScalarUDF for Pmod {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let PmodArgs { a, b } = inputs.try_into()?;
        pmod_impl(a, b)
    }

    fn name(&self) -> &'static str {
        "pmod"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let PmodArgs { a, b } = inputs.try_into()?;
        let a_field = a.to_field(schema)?;
        let b_field = b.to_field(schema)?;
        let dtype = pmod_supertype(&a_field.dtype, &b_field.dtype)?;
        Ok(Field::new(a_field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Returns the positive modulo: ((a % b) + b) % b. \
         Result has the same sign as b. Returns NULL if b is 0."
    }
}

fn pmod_supertype(a: &DataType, b: &DataType) -> DaftResult<DataType> {
    if !a.is_numeric() || !b.is_numeric() {
        return Err(DaftError::TypeError(format!(
            "Expected inputs to pmod to be numeric, got {a} and {b}"
        )));
    }
    (InferDataType::from(a) + InferDataType::from(b)).map(normalize_kernel_dtype)
}

// Collapse the supertype to one of {Int64, UInt64, Float32, Float64} so the
// kernel only has four code paths. Smaller int widths fit losslessly.
fn normalize_kernel_dtype(dtype: DataType) -> DataType {
    match dtype {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::UInt64,
        other => other,
    }
}

fn pmod_impl(a: Series, b: Series) -> DaftResult<Series> {
    let target = pmod_supertype(a.data_type(), b.data_type())?;
    let (a, b) = align_lengths(a.cast(&target)?, b.cast(&target)?)?;
    match target {
        DataType::Int64 => pmod_signed(a, b),
        DataType::UInt64 => pmod_unsigned(a, b),
        DataType::Float32 => pmod_f32(a, b),
        DataType::Float64 => pmod_f64(a, b),
        _ => unreachable!("pmod_supertype only returns int64/uint64/float32/float64"),
    }
}

fn align_lengths(a: Series, b: Series) -> DaftResult<(Series, Series)> {
    match (a.len(), b.len()) {
        (x, y) if x == y => Ok((a, b)),
        (1, n) => Ok((a.broadcast(n)?, b)),
        (n, 1) => Ok((a, b.broadcast(n)?)),
        (x, y) => Err(DaftError::ValueError(format!(
            "Cannot pmod arrays of different lengths: {x} vs {y}"
        ))),
    }
}

fn pmod_signed(a: Series, b: Series) -> DaftResult<Series> {
    let arr_a = a.i64().unwrap();
    let arr_b = b.i64().unwrap();
    // wrapping_* avoids debug-build panic on i64::MIN % -1 and on b == i64::MIN.
    let iter = arr_a.iter().zip(arr_b.iter()).map(|(x, y)| match (x, y) {
        (Some(x), Some(y)) if y != 0 => {
            let r = x.wrapping_rem(y);
            Some(r.wrapping_add(y).wrapping_rem(y))
        }
        _ => None,
    });
    Ok(Int64Array::from_iter(Field::new(arr_a.name(), DataType::Int64), iter).into_series())
}

fn pmod_unsigned(a: Series, b: Series) -> DaftResult<Series> {
    let arr_a = a.u64().unwrap();
    let arr_b = b.u64().unwrap();
    // Unsigned `%` is already non-negative, no sign correction needed.
    let iter = arr_a.iter().zip(arr_b.iter()).map(|(x, y)| match (x, y) {
        (Some(x), Some(y)) if y != 0 => Some(x % y),
        _ => None,
    });
    Ok(UInt64Array::from_iter(Field::new(arr_a.name(), DataType::UInt64), iter).into_series())
}

fn pmod_f32(a: Series, b: Series) -> DaftResult<Series> {
    let arr_a = a.f32().unwrap();
    let arr_b = b.f32().unwrap();
    let iter = arr_a.iter().zip(arr_b.iter()).map(|(x, y)| match (x, y) {
        (Some(x), Some(y)) if y != 0.0 => Some(x.rem_euclid(y)),
        _ => None,
    });
    Ok(Float32Array::from_iter(Field::new(arr_a.name(), DataType::Float32), iter).into_series())
}

fn pmod_f64(a: Series, b: Series) -> DaftResult<Series> {
    let arr_a = a.f64().unwrap();
    let arr_b = b.f64().unwrap();
    let iter = arr_a.iter().zip(arr_b.iter()).map(|(x, y)| match (x, y) {
        (Some(x), Some(y)) if y != 0.0 => Some(x.rem_euclid(y)),
        _ => None,
    });
    Ok(Float64Array::from_iter(Field::new(arr_a.name(), DataType::Float64), iter).into_series())
}

#[must_use]
pub fn pmod(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(Pmod, vec![a, b]).into()
}
