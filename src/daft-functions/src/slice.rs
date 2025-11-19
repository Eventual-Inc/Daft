use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    array::ListArray,
    prelude::{BinaryArray, DataType, Field, Int64Array, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Slice;

#[derive(FunctionArgs)]
struct SliceArgs<T> {
    input: T,
    start: T,
    #[arg(optional)]
    end: Option<T>,
}

#[typetag::serde]
impl ScalarUDF for Slice {
    fn name(&self) -> &'static str {
        "slice"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let SliceArgs { input, start, end } = args.try_into()?;

        let mut input_lengths = vec![input.len(), start.len()];
        if let Some(end) = &end {
            input_lengths.push(end.len());
        }

        let output_len = *input_lengths
            .iter()
            .filter(|l| **l != 1)
            .max()
            .unwrap_or(&1);
        for l in input_lengths {
            if !(l == 1 || l == output_len) {
                return Err(DaftError::ComputeError(format!(
                    "Input length mismatch in `slice` function: {output_len} vs {l}"
                )));
            }
        }

        if output_len == 0 {
            return Ok(Series::empty(input.name(), input.data_type()));
        }

        let start_casted = start.cast(&DataType::Int64)?;
        let start_arr = start_casted.i64()?;

        let end_casted = end.map(|e| e.cast(&DataType::Int64)).transpose()?;
        let end_arr = end_casted.as_ref().map(|e| e.i64()).transpose()?;

        match input.data_type() {
            DataType::List(_) => list_slice(
                input.name(),
                input.list()?.into_iter(),
                start_arr,
                end_arr,
                output_len,
            )
            .map(<_>::into_series),
            DataType::FixedSizeList(..) => list_slice(
                input.name(),
                input.fixed_size_list()?.into_iter(),
                start_arr,
                end_arr,
                output_len,
            )
            .map(<_>::into_series),
            DataType::Binary => Ok(binary_slice(
                input.name(),
                input.binary()?.into_iter(),
                start_arr,
                end_arr,
                output_len,
            )
            .into_series()),
            DataType::FixedSizeBinary(_) => Ok(binary_slice(
                input.name(),
                input.fixed_size_binary()?.into_iter(),
                start_arr,
                end_arr,
                output_len,
            )
            .into_series()),
            DataType::Null => Ok(input.clone()),
            dtype => Err(DaftError::TypeError(format!(
                "Input to `slice` must be list or binary, received: {dtype}"
            ))),
        }
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let SliceArgs { input, start, end } = args.try_into()?;

        let start_type = start.get_type(schema)?;
        ensure!(
            start_type.is_integer() || start_type.is_null(),
            TypeError: "`start` argument to `slice` must be an integer, received: {start_type}",
        );

        if let Some(end) = end {
            let end_type = end.get_type(schema)?;
            ensure!(
                end_type.is_integer() || end_type.is_null(),
                TypeError: "`end` argument to `slice` must be an integer, received: {end_type}",
            );
        }

        let input_field = input.to_field(schema)?;
        let output_dtype = match input_field.dtype {
            DataType::List(inner_dtype) | DataType::FixedSizeList(inner_dtype, _) => {
                DataType::List(inner_dtype)
            }
            DataType::Binary | DataType::FixedSizeBinary(_) => DataType::Binary,
            DataType::Null => DataType::Null,
            dtype => {
                return Err(DaftError::TypeError(format!(
                    "Input to `slice` must be list or binary, received: {dtype}"
                )));
            }
        };

        Ok(Field::new(input_field.name, output_dtype))
    }
}

pub fn slice(expr: ExprRef, start: ExprRef, end: ExprRef) -> ExprRef {
    ScalarFn::builtin(Slice {}, vec![expr, start, end]).into()
}

/// Converts negative indices to non-negative and bounds indices to (0..len)
fn bound_index(idx: i64, len: usize) -> usize {
    if idx >= 0 {
        std::cmp::min(idx as usize, len)
    } else {
        std::cmp::max((len as i64) + idx, 0) as usize
    }
}

fn list_slice(
    name: &str,
    input_iter: impl Iterator<Item = Option<Series>> + Clone,
    start: &Int64Array,
    end: Option<&Int64Array>,
    output_len: usize,
) -> DaftResult<ListArray> {
    let input_iter = input_iter.cycle().take(output_len);
    let start_iter = start.into_iter().cycle().take(output_len);

    let end_iter: Box<dyn Iterator<Item = Option<i64>>> = if let Some(end) = end {
        Box::new(end.into_iter().map(|i| i.copied()).cycle().take(output_len))
    } else {
        Box::new(input_iter.clone().map(|i| i.map(|i| i.len() as i64)))
    };

    let slices = start_iter
        .zip(end_iter)
        .zip(input_iter)
        .map(|((s, e), i)| {
            let (Some(s), Some(e), Some(i)) = (s.copied(), e, i) else {
                return Ok(None);
            };

            let s = bound_index(s, i.len());
            let e = bound_index(e, i.len());

            if s >= e {
                Ok(Some(Series::empty(i.name(), i.data_type())))
            } else {
                i.slice(s, e).map(Some)
            }
        })
        .collect::<DaftResult<Vec<_>>>()?;

    ListArray::try_from((name, slices.as_slice()))
}

fn binary_slice<'a>(
    name: &str,
    input_iter: impl Iterator<Item = Option<&'a [u8]>> + Clone,
    start: &Int64Array,
    end: Option<&Int64Array>,
    output_len: usize,
) -> BinaryArray {
    let input_iter = input_iter.cycle().take(output_len);
    let start_iter = start.into_iter().cycle().take(output_len);

    let end_iter: Box<dyn Iterator<Item = Option<i64>>> = if let Some(end) = end {
        Box::new(end.into_iter().map(|i| i.copied()).cycle().take(output_len))
    } else {
        Box::new(input_iter.clone().map(|i| i.map(|i| i.len() as i64)))
    };

    let slices = start_iter
        .zip(end_iter)
        .zip(input_iter)
        .map(|((s, e), i)| {
            let (Some(s), Some(e), Some(i)) = (s.copied(), e, i) else {
                return None;
            };

            let s = bound_index(s, i.len());
            let e = bound_index(e, i.len());

            if s >= e {
                Some(&[] as &[u8])
            } else {
                Some(&i[s..e])
            }
        })
        .collect::<Vec<_>>();

    BinaryArray::from_iter(name, slices.into_iter())
}
