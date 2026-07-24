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

use super::pmod::{align_lengths, normalize_kernel_dtype};

#[derive(FunctionArgs)]
struct TryArithmeticArgs<T> {
    left: T,
    right: T,
}

#[derive(Clone, Copy)]
enum CheckedOp {
    Add,
    Sub,
    Mul,
}

impl CheckedOp {
    fn fn_name(self) -> &'static str {
        match self {
            Self::Add => "try_add",
            Self::Sub => "try_subtract",
            Self::Mul => "try_multiply",
        }
    }

    fn infer(self, l: &DataType, r: &DataType) -> DaftResult<DataType> {
        let (l, r) = (InferDataType::from(l), InferDataType::from(r));
        match self {
            Self::Add => l + r,
            Self::Sub => l - r,
            Self::Mul => l * r,
        }
    }

    fn i64(self, x: i64, y: i64) -> Option<i64> {
        match self {
            Self::Add => x.checked_add(y),
            Self::Sub => x.checked_sub(y),
            Self::Mul => x.checked_mul(y),
        }
    }

    fn u64(self, x: u64, y: u64) -> Option<u64> {
        match self {
            Self::Add => x.checked_add(y),
            Self::Sub => x.checked_sub(y),
            Self::Mul => x.checked_mul(y),
        }
    }

    fn f32(self, x: f32, y: f32) -> f32 {
        match self {
            Self::Add => x + y,
            Self::Sub => x - y,
            Self::Mul => x * y,
        }
    }

    fn f64(self, x: f64, y: f64) -> f64 {
        match self {
            Self::Add => x + y,
            Self::Sub => x - y,
            Self::Mul => x * y,
        }
    }
}

fn is_unsigned(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    )
}

/// The 64-bit type the checked kernels compute in. Integer pairs always compute in
/// 64-bit integers, including mixed signed/unsigned pairs, which the regular arithmetic
/// inference would promote to Float64 and thereby silently skip the overflow check.
fn checked_target(op: CheckedOp, left: &DataType, right: &DataType) -> DaftResult<DataType> {
    if !left.is_numeric() || !right.is_numeric() {
        return Err(DaftError::TypeError(format!(
            "Expected inputs to {} to be numeric, got {} and {}",
            op.fn_name(),
            left,
            right
        )));
    }
    if left.is_integer() && right.is_integer() {
        if is_unsigned(left) && is_unsigned(right) {
            Ok(DataType::UInt64)
        } else {
            Ok(DataType::Int64)
        }
    } else {
        op.infer(left, right).map(normalize_kernel_dtype)
    }
}

fn try_arithmetic_field(
    op: CheckedOp,
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
) -> DaftResult<Field> {
    let TryArithmeticArgs { left, right } = inputs.try_into()?;
    let left_field = left.to_field(schema)?;
    let right_field = right.to_field(schema)?;
    let dtype = checked_target(op, &left_field.dtype, &right_field.dtype)?;
    Ok(Field::new(left_field.name, dtype))
}

fn try_arithmetic_impl(op: CheckedOp, left: Series, right: Series) -> DaftResult<Series> {
    let target = checked_target(op, left.data_type(), right.data_type())?;
    // A signed/UInt64 pair has no common 64-bit integer type: keep the UInt64 side
    // unsigned and range-check each value into Int64, so values above i64::MAX null
    // out like any other overflow instead of wrapping through a cast.
    let left_is_u64 = matches!(left.data_type(), DataType::UInt64);
    let right_is_u64 = matches!(right.data_type(), DataType::UInt64);
    if target == DataType::Int64 && (left_is_u64 || right_is_u64) {
        let (l, r) = align_lengths(
            left.cast(if left_is_u64 {
                &DataType::UInt64
            } else {
                &DataType::Int64
            })?,
            right.cast(if right_is_u64 {
                &DataType::UInt64
            } else {
                &DataType::Int64
            })?,
        )?;
        let iter: Box<dyn Iterator<Item = Option<i64>>> = if left_is_u64 {
            let (a, b) = (l.u64().unwrap(), r.i64().unwrap());
            Box::new(a.iter().zip(b.iter()).map(move |(x, y)| match (x, y) {
                (Some(x), Some(y)) => i64::try_from(x).ok().and_then(|x| op.i64(x, y)),
                _ => None,
            }))
        } else {
            let (a, b) = (l.i64().unwrap(), r.u64().unwrap());
            Box::new(a.iter().zip(b.iter()).map(move |(x, y)| match (x, y) {
                (Some(x), Some(y)) => i64::try_from(y).ok().and_then(|y| op.i64(x, y)),
                _ => None,
            }))
        };
        return Ok(
            Int64Array::from_iter(Field::new(l.name(), DataType::Int64), iter).into_series(),
        );
    }
    let (left, right) = align_lengths(left.cast(&target)?, right.cast(&target)?)?;
    let result = match target {
        DataType::Int64 => {
            let (a, b) = (left.i64().unwrap(), right.i64().unwrap());
            let iter = a.iter().zip(b.iter()).map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => op.i64(x, y),
                _ => None,
            });
            Int64Array::from_iter(Field::new(a.name(), DataType::Int64), iter).into_series()
        }
        DataType::UInt64 => {
            let (a, b) = (left.u64().unwrap(), right.u64().unwrap());
            let iter = a.iter().zip(b.iter()).map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => op.u64(x, y),
                _ => None,
            });
            UInt64Array::from_iter(Field::new(a.name(), DataType::UInt64), iter).into_series()
        }
        // Floats do not overflow (they saturate to +/-inf), so no NULLs are introduced.
        DataType::Float32 => {
            let (a, b) = (left.f32().unwrap(), right.f32().unwrap());
            let iter = a.iter().zip(b.iter()).map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => Some(op.f32(x, y)),
                _ => None,
            });
            Float32Array::from_iter(Field::new(a.name(), DataType::Float32), iter).into_series()
        }
        DataType::Float64 => {
            let (a, b) = (left.f64().unwrap(), right.f64().unwrap());
            let iter = a.iter().zip(b.iter()).map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => Some(op.f64(x, y)),
                _ => None,
            });
            Float64Array::from_iter(Field::new(a.name(), DataType::Float64), iter).into_series()
        }
        other => {
            return Err(DaftError::TypeError(format!(
                "{} cannot compute in {}",
                op.fn_name(),
                other
            )));
        }
    };
    Ok(result)
}

macro_rules! impl_try_arithmetic {
    ($struct_name:ident, $op:expr, $doc:expr) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $struct_name;

        #[typetag::serde]
        impl ScalarUDF for $struct_name {
            fn call(
                &self,
                inputs: FunctionArgs<Series>,
                _ctx: &daft_dsl::functions::scalar::EvalContext,
            ) -> DaftResult<Series> {
                let TryArithmeticArgs { left, right } = inputs.try_into()?;
                try_arithmetic_impl($op, left, right)
            }

            fn name(&self) -> &'static str {
                $op.fn_name()
            }

            fn get_return_field(
                &self,
                inputs: FunctionArgs<ExprRef>,
                schema: &Schema,
            ) -> DaftResult<Field> {
                try_arithmetic_field($op, inputs, schema)
            }

            fn docstring(&self) -> &'static str {
                $doc
            }
        }
    };
}

impl_try_arithmetic!(
    TryAdd,
    CheckedOp::Add,
    "Adds two numbers, returning NULL on integer overflow instead of wrapping."
);
impl_try_arithmetic!(
    TrySubtract,
    CheckedOp::Sub,
    "Subtracts two numbers, returning NULL on integer overflow instead of wrapping."
);
impl_try_arithmetic!(
    TryMultiply,
    CheckedOp::Mul,
    "Multiplies two numbers, returning NULL on integer overflow instead of wrapping."
);

#[must_use]
pub fn try_add(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(TryAdd {}, vec![left, right]).into()
}

#[must_use]
pub fn try_subtract(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(TrySubtract {}, vec![left, right]).into()
}

#[must_use]
pub fn try_multiply(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(TryMultiply {}, vec![left, right]).into()
}
