use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{InferDataType, Int64Array, UInt64Array},
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
}

impl ArithmeticOp {
    fn checked_i64(self, lhs: i64, rhs: i64) -> Option<i64> {
        match self {
            Self::Add => lhs.checked_add(rhs),
            Self::Subtract => lhs.checked_sub(rhs),
            Self::Multiply => lhs.checked_mul(rhs),
        }
    }

    fn checked_u64(self, lhs: u64, rhs: u64) -> Option<u64> {
        match self {
            Self::Add => lhs.checked_add(rhs),
            Self::Subtract => lhs.checked_sub(rhs),
            Self::Multiply => lhs.checked_mul(rhs),
        }
    }
}

#[derive(FunctionArgs)]
struct ArithmeticArgs<T> {
    a: T,
    b: T,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TryAdd;

#[typetag::serde]
impl ScalarUDF for TryAdd {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ArithmeticArgs { a, b } = inputs.try_into()?;
        try_arithmetic_impl(a, b, self.name(), ArithmeticOp::Add)
    }

    fn name(&self) -> &'static str {
        "try_add"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        try_arithmetic_field(inputs, schema, self.name())
    }

    fn docstring(&self) -> &'static str {
        "Adds two numbers, returning NULL where the 64-bit integer result would overflow."
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TrySubtract;

#[typetag::serde]
impl ScalarUDF for TrySubtract {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ArithmeticArgs { a, b } = inputs.try_into()?;
        try_arithmetic_impl(a, b, self.name(), ArithmeticOp::Subtract)
    }

    fn name(&self) -> &'static str {
        "try_subtract"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        try_arithmetic_field(inputs, schema, self.name())
    }

    fn docstring(&self) -> &'static str {
        "Subtracts b from a, returning NULL where the 64-bit integer result would overflow \
         or an unsigned result would be negative."
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TryMultiply;

#[typetag::serde]
impl ScalarUDF for TryMultiply {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ArithmeticArgs { a, b } = inputs.try_into()?;
        try_arithmetic_impl(a, b, self.name(), ArithmeticOp::Multiply)
    }

    fn name(&self) -> &'static str {
        "try_multiply"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        try_arithmetic_field(inputs, schema, self.name())
    }

    fn docstring(&self) -> &'static str {
        "Multiplies two numbers, returning NULL where the 64-bit integer result would overflow."
    }
}

fn try_arithmetic_field(
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
    fn_name: &str,
) -> DaftResult<Field> {
    let ArithmeticArgs { a, b } = inputs.try_into()?;
    let a_field = a.to_field(schema)?;
    let b_field = b.to_field(schema)?;
    let dtype = arithmetic_supertype(&a_field.dtype, &b_field.dtype, fn_name)?;
    Ok(Field::new(a_field.name, dtype))
}

fn arithmetic_supertype(a: &DataType, b: &DataType, fn_name: &str) -> DaftResult<DataType> {
    // A null literal or all-null column takes its type from the numeric
    // operand, matching ordinary arithmetic. Two null inputs have nothing to
    // infer a kernel from.
    if matches!((a, b), (DataType::Null, DataType::Null)) {
        return Err(DaftError::TypeError(format!(
            "Expected at least one numeric input to {fn_name}, got null for both"
        )));
    }
    let is_numeric_or_null = |dtype: &DataType| dtype.is_numeric() || *dtype == DataType::Null;
    if !is_numeric_or_null(a) || !is_numeric_or_null(b) {
        return Err(DaftError::TypeError(format!(
            "Expected inputs to {fn_name} to be numeric, got {a} and {b}"
        )));
    }
    (InferDataType::from(a) + InferDataType::from(b)).map(normalize_kernel_dtype)
}

// Collapse the supertype to one of {Int64, UInt64, Float16, Float32, Float64}
// so the kernel only has a handful of code paths. Smaller int widths fit
// losslessly in the 64-bit variant, which gives intermediate results room to
// grow before they are checked.
fn normalize_kernel_dtype(dtype: DataType) -> DataType {
    match dtype {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::UInt64,
        other => other,
    }
}

fn align_lengths(a: Series, b: Series) -> DaftResult<(Series, Series)> {
    match (a.len(), b.len()) {
        (x, y) if x == y => Ok((a, b)),
        (1, n) => Ok((a.broadcast(n)?, b)),
        (n, 1) => Ok((a, b.broadcast(n)?)),
        (x, y) => Err(DaftError::ValueError(format!(
            "Cannot apply try arithmetic to arrays of different lengths: {x} vs {y}"
        ))),
    }
}

fn try_arithmetic_impl(
    a: Series,
    b: Series,
    fn_name: &str,
    op: ArithmeticOp,
) -> DaftResult<Series> {
    let target = arithmetic_supertype(a.data_type(), b.data_type(), fn_name)?;
    let (a, b) = align_lengths(a.cast(&target)?, b.cast(&target)?)?;
    match target {
        DataType::Int64 => {
            let lhs = a.i64().unwrap();
            let rhs = b.i64().unwrap();
            let iter = lhs.iter().zip(rhs.iter()).map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => op.checked_i64(x, y),
                _ => None,
            });
            Ok(Int64Array::from_iter(Field::new(lhs.name(), DataType::Int64), iter).into_series())
        }
        DataType::UInt64 => {
            let lhs = a.u64().unwrap();
            let rhs = b.u64().unwrap();
            let iter = lhs.iter().zip(rhs.iter()).map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => op.checked_u64(x, y),
                _ => None,
            });
            Ok(
                UInt64Array::from_iter(Field::new(lhs.name(), DataType::UInt64), iter)
                    .into_series(),
            )
        }
        // Floats (and mixed signed/unsigned integers, which Daft promotes to
        // Float64) follow IEEE semantics: overflow produces inf, not NULL.
        _ => match op {
            ArithmeticOp::Add => &a + &b,
            ArithmeticOp::Subtract => &a - &b,
            ArithmeticOp::Multiply => &a * &b,
        },
    }
}

#[must_use]
pub fn try_add(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(TryAdd {}, vec![a, b]).into()
}

#[must_use]
pub fn try_subtract(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(TrySubtract {}, vec![a, b]).into()
}

#[must_use]
pub fn try_multiply(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(TryMultiply {}, vec![a, b]).into()
}
