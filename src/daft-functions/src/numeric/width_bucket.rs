use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::Int64Array,
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct WidthBucket;

#[derive(FunctionArgs)]
struct WidthBucketArgs<T> {
    value: T,
    min: T,
    max: T,
    num_bucket: T,
}

#[typetag::serde]
impl ScalarUDF for WidthBucket {
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let WidthBucketArgs {
            value,
            min,
            max,
            num_bucket,
        } = inputs.try_into()?;
        width_bucket_impl(value, min, max, num_bucket)
    }

    fn name(&self) -> &'static str {
        "width_bucket"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let WidthBucketArgs {
            value,
            min,
            max,
            num_bucket,
        } = inputs.try_into()?;
        let value_field = value.to_field(schema)?;
        let min_field = min.to_field(schema)?;
        let max_field = max.to_field(schema)?;
        let num_bucket_field = num_bucket.to_field(schema)?;
        for (name, field) in [
            ("value", &value_field),
            ("min", &min_field),
            ("max", &max_field),
            ("num_bucket", &num_bucket_field),
        ] {
            if !field.dtype.is_numeric() {
                return Err(DaftError::TypeError(format!(
                    "Expected `{name}` of width_bucket to be numeric, got {}",
                    field.dtype
                )));
            }
        }
        Ok(Field::new(value_field.name, DataType::Int64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the bucket number for `value` in an equiwidth histogram with \
         `num_bucket` buckets in the range [min, max]. Returns 0 below the range and \
         num_bucket+1 at or above. Supports descending bounds (min > max). Returns NULL \
         if num_bucket <= 0, min == max, value/min/max is NaN, or min/max is infinite."
    }
}

fn width_bucket_impl(
    value: Series,
    min: Series,
    max: Series,
    num_bucket: Series,
) -> DaftResult<Series> {
    let value = value.cast(&DataType::Float64)?;
    let min = min.cast(&DataType::Float64)?;
    let max = max.cast(&DataType::Float64)?;
    let num_bucket = num_bucket.cast(&DataType::Int64)?;
    let (value, min, max, num_bucket) = align_lengths(value, min, max, num_bucket)?;

    let v_arr = value.f64().unwrap();
    let mn_arr = min.f64().unwrap();
    let mx_arr = max.f64().unwrap();
    let nb_arr = num_bucket.i64().unwrap();

    let iter = v_arr
        .iter()
        .zip(mn_arr.iter())
        .zip(mx_arr.iter())
        .zip(nb_arr.iter())
        .map(|(((v, mn), mx), nb)| match (v, mn, mx, nb) {
            (Some(v), Some(mn), Some(mx), Some(nb)) => compute_bucket(v, mn, mx, nb),
            _ => None,
        });
    Ok(Int64Array::from_iter(Field::new(v_arr.name(), DataType::Int64), iter).into_series())
}

fn align_lengths(
    a: Series,
    b: Series,
    c: Series,
    d: Series,
) -> DaftResult<(Series, Series, Series, Series)> {
    let lens = [a.len(), b.len(), c.len(), d.len()];
    let max_len = *lens.iter().max().unwrap();
    for &l in &lens {
        if l != 1 && l != max_len {
            return Err(DaftError::ValueError(format!(
                "Cannot apply width_bucket to arrays of different lengths: {} vs {} vs {} vs {}",
                lens[0], lens[1], lens[2], lens[3]
            )));
        }
    }
    let bcast = |s: Series| -> DaftResult<Series> {
        if s.len() == max_len {
            Ok(s)
        } else {
            s.broadcast(max_len)
        }
    };
    Ok((bcast(a)?, bcast(b)?, bcast(c)?, bcast(d)?))
}

/// Mirrors Spark's `WidthBucket` NULL and bucketing semantics for parity.
fn compute_bucket(v: f64, mn: f64, mx: f64, nb: i64) -> Option<i64> {
    if nb <= 0
        || nb == i64::MAX
        || v.is_nan()
        || mn == mx
        || mn.is_nan()
        || mn.is_infinite()
        || mx.is_nan()
        || mx.is_infinite()
    {
        return None;
    }
    let bucket = if mn < mx {
        if v < mn {
            0
        } else if v >= mx {
            nb + 1
        } else {
            ((nb as f64) * (v - mn) / (mx - mn)) as i64 + 1
        }
    } else {
        // Descending: roles of below/above flip so a smaller value sorts higher.
        if v > mn {
            0
        } else if v <= mx {
            nb + 1
        } else {
            ((nb as f64) * (mn - v) / (mn - mx)) as i64 + 1
        }
    };
    Some(bucket)
}

#[must_use]
pub fn width_bucket(value: ExprRef, min: ExprRef, max: ExprRef, num_bucket: ExprRef) -> ExprRef {
    ScalarFn::builtin(WidthBucket, vec![value, min, max, num_bucket]).into()
}
