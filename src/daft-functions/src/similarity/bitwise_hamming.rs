use std::sync::Arc;

use arrow_buffer::NullBufferBuilder;
use daft_common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BitwiseHammingDistanceFunction;

#[typetag::serde]
impl ScalarUDF for BitwiseHammingDistanceFunction {
    fn name(&self) -> &'static str {
        "hamming_distance"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let left = inputs.required(0)?;
        let right = inputs.required(1)?;

        match (left.data_type(), right.data_type()) {
            (DataType::UInt64, DataType::UInt64) => compute_u64(left, right),
            (DataType::FixedSizeBinary(n1), DataType::FixedSizeBinary(n2)) if n1 == n2 => {
                compute_fixed_size_binary(left, right)
            }
            (l, r) if l.is_integer() && r.is_integer() => {
                let left = left.cast(&DataType::UInt64)?;
                let right = right.cast(&DataType::UInt64)?;
                compute_u64(&left, &right)
            }
            (l, r) => Err(DaftError::TypeError(format!(
                "hamming_distance requires both inputs to be integer types or \
                 FixedSizeBinary of matching size, got {} and {}",
                l, r
            ))),
        }
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 inputs, but received {}",
            inputs.len()
        );

        let left = inputs.required(0)?.to_field(schema)?;
        let right = inputs.required(1)?.to_field(schema)?;

        match (&left.dtype, &right.dtype) {
            (DataType::UInt64, DataType::UInt64) => {}
            (DataType::FixedSizeBinary(n1), DataType::FixedSizeBinary(n2)) => {
                ensure!(
                    n1 == n2,
                    TypeError: "FixedSizeBinary sizes must match, got {} and {}",
                    n1, n2
                );
            }
            (l, r) if l.is_integer() && r.is_integer() => {}
            (l, r) => {
                return Err(DaftError::TypeError(format!(
                    "hamming_distance requires both inputs to be integer types or \
                     FixedSizeBinary of matching size, got {} and {}",
                    l, r
                )));
            }
        }

        Ok(Field::new(left.name, DataType::UInt32))
    }

    fn docstring(&self) -> &'static str {
        "Compute the bitwise Hamming distance (popcount of XOR) between two hash \
        fingerprints. Supports integer and FixedSizeBinary inputs."
    }
}

fn compute_u64(left: &Series, right: &Series) -> DaftResult<Series> {
    let left = left.cast(&DataType::UInt64)?;
    let right = right.cast(&DataType::UInt64)?;
    let left_arr = left.u64()?;
    let right_arr = right.u64()?;

    let (longer, shorter, len) = if left_arr.len() >= right_arr.len() {
        (left_arr, right_arr, left_arr.len())
    } else {
        (right_arr, left_arr, right_arr.len())
    };

    let broadcast = shorter.len() == 1;
    if !broadcast && shorter.len() != len {
        return Err(DaftError::ValueError(format!(
            "Inputs must have the same length or one must be a scalar, got {} and {}",
            left_arr.len(),
            right_arr.len()
        )));
    }

    let mut values = Vec::with_capacity(len);
    let mut validity = NullBufferBuilder::new(len);

    for (i, l_opt) in longer.iter().enumerate() {
        let r_opt = if broadcast {
            shorter.get(0)
        } else {
            shorter.get(i)
        };
        match (l_opt, r_opt) {
            (Some(l), Some(r)) => {
                values.push((l ^ r).count_ones());
                validity.append_non_null();
            }
            _ => {
                values.push(0);
                validity.append_null();
            }
        }
    }

    let field = Arc::new(Field::new(longer.name(), DataType::UInt32));
    let result = UInt32Array::from_field_and_values(field, values).with_nulls(validity.finish())?;
    Ok(result.into_series())
}

fn compute_fixed_size_binary(left: &Series, right: &Series) -> DaftResult<Series> {
    let left_arr = left.fixed_size_binary()?;
    let right_arr = right.fixed_size_binary()?;

    let (longer, shorter, len) = if left_arr.len() >= right_arr.len() {
        (left_arr, right_arr, left_arr.len())
    } else {
        (right_arr, left_arr, right_arr.len())
    };

    let broadcast = shorter.len() == 1;
    if !broadcast && shorter.len() != len {
        return Err(DaftError::ValueError(format!(
            "Inputs must have the same length or one must be a scalar, got {} and {}",
            left_arr.len(),
            right_arr.len()
        )));
    }

    let mut values = Vec::with_capacity(len);
    let mut validity = NullBufferBuilder::new(len);

    for (i, l_opt) in longer.iter().enumerate() {
        let r_opt = if broadcast {
            shorter.get(0)
        } else {
            shorter.get(i)
        };
        match (l_opt, r_opt) {
            (Some(l_bytes), Some(r_bytes)) => {
                let dist: u32 = l_bytes
                    .iter()
                    .zip(r_bytes.iter())
                    .map(|(a, b)| (a ^ b).count_ones())
                    .sum();
                values.push(dist);
                validity.append_non_null();
            }
            _ => {
                values.push(0);
                validity.append_null();
            }
        }
    }

    let field = Arc::new(Field::new(longer.name(), DataType::UInt32));
    let result = UInt32Array::from_field_and_values(field, values).with_nulls(validity.finish())?;
    Ok(result.into_series())
}

#[must_use]
pub fn hamming_distance(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(BitwiseHammingDistanceFunction, vec![left, right]).into()
}
