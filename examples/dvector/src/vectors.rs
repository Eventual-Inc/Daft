/// Arrow vector column abstractions for float and boolean list arrays.
///
/// Provides unified views over FixedSizeList, List, and LargeList columns
/// and map helpers for pairwise distance computations.
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, GenericListArray,
    builder::{Float64Builder, UInt32Builder},
    cast::AsArray,
    types::{Float32Type, Float64Type},
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use daft_ext::prelude::{DaftError, DaftResult};

/// Validates that two list-typed args have compatible element types and dimensions.
pub fn return_field_float(fn_name: &str, args: &[Field]) -> DaftResult<Field> {
    validate_two_args(fn_name, args)?;
    let float_types = [DataType::Float32, DataType::Float64];
    validate_list_element(&args[0], fn_name, &float_types)?;
    validate_list_element(&args[1], fn_name, &float_types)?;
    validate_fixed_dims_match(fn_name, &args[0], &args[1])?;
    Ok(Field::new(fn_name, DataType::Float64, true))
}

/// Validates boolean list args, returns UInt32 field.
pub fn return_field_hamming(fn_name: &str, args: &[Field]) -> DaftResult<Field> {
    validate_two_args(fn_name, args)?;
    let bool_types = [DataType::Boolean];
    validate_list_element(&args[0], fn_name, &bool_types)?;
    validate_list_element(&args[1], fn_name, &bool_types)?;
    validate_fixed_dims_match(fn_name, &args[0], &args[1])?;
    Ok(Field::new(fn_name, DataType::UInt32, true))
}

/// Validates boolean list args, returns Float64 field.
pub fn return_field_jaccard(fn_name: &str, args: &[Field]) -> DaftResult<Field> {
    validate_two_args(fn_name, args)?;
    let bool_types = [DataType::Boolean];
    validate_list_element(&args[0], fn_name, &bool_types)?;
    validate_list_element(&args[1], fn_name, &bool_types)?;
    validate_fixed_dims_match(fn_name, &args[0], &args[1])?;
    Ok(Field::new(fn_name, DataType::Float64, true))
}

fn validate_list_element(
    field: &Field,
    fn_name: &str,
    allowed: &[DataType],
) -> DaftResult<DataType> {
    match field.data_type() {
        DataType::FixedSizeList(inner, _) | DataType::List(inner) | DataType::LargeList(inner) => {
            let elem = inner.data_type().clone();
            if allowed.contains(&elem) {
                Ok(elem)
            } else {
                Err(DaftError::TypeError(format!(
                    "{fn_name}: unsupported element type {elem:?}, expected one of {allowed:?}"
                )))
            }
        }
        other => Err(DaftError::TypeError(format!(
            "{fn_name}: expected List or FixedSizeList, got {other:?}"
        ))),
    }
}

fn validate_two_args(fn_name: &str, args: &[Field]) -> DaftResult<()> {
    if args.len() != 2 {
        return Err(DaftError::TypeError(format!(
            "{fn_name}: expected 2 arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

fn validate_fixed_dims_match(fn_name: &str, a: &Field, b: &Field) -> DaftResult<()> {
    if let (DataType::FixedSizeList(_, da), DataType::FixedSizeList(_, db)) =
        (a.data_type(), b.data_type())
        && da != db
    {
        return Err(DaftError::TypeError(format!(
            "{fn_name}: dimension mismatch: {da} vs {db}"
        )));
    }
    Ok(())
}

/// Borrowed or owned f64 slice over float array values.
enum F64Values<'a> {
    Borrowed(&'a [f64]),
    Owned(Vec<f64>),
}

impl<'a> F64Values<'a> {
    fn as_slice(&self) -> &[f64] {
        match self {
            F64Values::Borrowed(s) => s,
            F64Values::Owned(v) => v.as_slice(),
        }
    }
}

enum VectorLayout<'a> {
    Fixed(i32),
    Variable32(&'a OffsetBuffer<i32>),
    Variable64(&'a OffsetBuffer<i64>),
}

/// Unified view over a column of float vectors (FixedSizeList, List, or LargeList).
struct FloatVectorColumn<'a> {
    values: F64Values<'a>,
    layout: VectorLayout<'a>,
    nulls: Option<&'a NullBuffer>,
    len: usize,
}

impl<'a> FloatVectorColumn<'a> {
    fn try_from_array(array: &'a ArrayRef) -> DaftResult<Self> {
        match array.data_type() {
            DataType::FixedSizeList(inner, dim) => {
                let fsl = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| DaftError::TypeError("expected FixedSizeListArray".into()))?;
                let values = extract_float_values(fsl.values(), inner.data_type())?;
                Ok(FloatVectorColumn {
                    values,
                    layout: VectorLayout::Fixed(*dim),
                    nulls: fsl.nulls(),
                    len: fsl.len(),
                })
            }
            DataType::List(inner) => {
                let list = array
                    .as_any()
                    .downcast_ref::<GenericListArray<i32>>()
                    .ok_or_else(|| DaftError::TypeError("expected ListArray".into()))?;
                let values = extract_float_values(list.values(), inner.data_type())?;
                Ok(FloatVectorColumn {
                    values,
                    layout: VectorLayout::Variable32(list.offsets()),
                    nulls: list.nulls(),
                    len: list.len(),
                })
            }
            DataType::LargeList(inner) => {
                let list = array
                    .as_any()
                    .downcast_ref::<GenericListArray<i64>>()
                    .ok_or_else(|| DaftError::TypeError("expected LargeListArray".into()))?;
                let values = extract_float_values(list.values(), inner.data_type())?;
                Ok(FloatVectorColumn {
                    values,
                    layout: VectorLayout::Variable64(list.offsets()),
                    nulls: list.nulls(),
                    len: list.len(),
                })
            }
            other => Err(DaftError::TypeError(format!(
                "expected List or FixedSizeList, got {other:?}"
            ))),
        }
    }

    fn get(&self, i: usize) -> Option<&[f64]> {
        if self.nulls.is_some_and(|nulls| !nulls.is_valid(i)) {
            return None;
        }
        let all = self.values.as_slice();
        match &self.layout {
            VectorLayout::Fixed(dim) => {
                let d = *dim as usize;
                let start = i * d;
                Some(&all[start..start + d])
            }
            VectorLayout::Variable32(offsets) => {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                Some(&all[start..end])
            }
            VectorLayout::Variable64(offsets) => {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                Some(&all[start..end])
            }
        }
    }
}

fn extract_float_values<'a>(
    values: &'a ArrayRef,
    elem_type: &DataType,
) -> DaftResult<F64Values<'a>> {
    match elem_type {
        DataType::Float64 => {
            let arr = values.as_primitive::<Float64Type>();
            Ok(F64Values::Borrowed(arr.values().as_ref()))
        }
        DataType::Float32 => {
            let arr = values.as_primitive::<Float32Type>();
            let promoted: Vec<f64> = arr.values().iter().map(|v| *v as f64).collect();
            Ok(F64Values::Owned(promoted))
        }
        other => Err(DaftError::TypeError(format!(
            "unsupported element type {other:?}, expected Float32 or Float64"
        ))),
    }
}

/// Unified view over a column of boolean vectors (FixedSizeList, List, or LargeList).
struct BoolVectorColumn<'a> {
    array: &'a dyn Array,
    layout: BoolLayout<'a>,
    nulls: Option<&'a NullBuffer>,
    len: usize,
}

enum BoolLayout<'a> {
    Fixed(i32),
    Variable32(&'a OffsetBuffer<i32>),
    Variable64(&'a OffsetBuffer<i64>),
}

impl<'a> BoolVectorColumn<'a> {
    fn try_from_array(array: &'a ArrayRef) -> DaftResult<Self> {
        match array.data_type() {
            DataType::FixedSizeList(inner, dim) => {
                if *inner.data_type() != DataType::Boolean {
                    return Err(DaftError::TypeError(format!(
                        "expected Boolean elements, got {:?}",
                        inner.data_type()
                    )));
                }
                let fsl = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| DaftError::TypeError("expected FixedSizeListArray".into()))?;
                Ok(BoolVectorColumn {
                    array: fsl.values().as_ref(),
                    layout: BoolLayout::Fixed(*dim),
                    nulls: fsl.nulls(),
                    len: fsl.len(),
                })
            }
            DataType::List(inner) => {
                if *inner.data_type() != DataType::Boolean {
                    return Err(DaftError::TypeError(format!(
                        "expected Boolean elements, got {:?}",
                        inner.data_type()
                    )));
                }
                let list = array
                    .as_any()
                    .downcast_ref::<GenericListArray<i32>>()
                    .ok_or_else(|| DaftError::TypeError("expected ListArray".into()))?;
                Ok(BoolVectorColumn {
                    array: list.values().as_ref(),
                    layout: BoolLayout::Variable32(list.offsets()),
                    nulls: list.nulls(),
                    len: list.len(),
                })
            }
            DataType::LargeList(inner) => {
                if *inner.data_type() != DataType::Boolean {
                    return Err(DaftError::TypeError(format!(
                        "expected Boolean elements, got {:?}",
                        inner.data_type()
                    )));
                }
                let list = array
                    .as_any()
                    .downcast_ref::<GenericListArray<i64>>()
                    .ok_or_else(|| DaftError::TypeError("expected LargeListArray".into()))?;
                Ok(BoolVectorColumn {
                    array: list.values().as_ref(),
                    layout: BoolLayout::Variable64(list.offsets()),
                    nulls: list.nulls(),
                    len: list.len(),
                })
            }
            other => Err(DaftError::TypeError(format!(
                "expected List or FixedSizeList of Boolean, got {other:?}"
            ))),
        }
    }

    fn get(&self, i: usize) -> Option<BooleanArray> {
        if self.nulls.is_some_and(|nulls| !nulls.is_valid(i)) {
            return None;
        }
        let bool_arr = self
            .array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("inner array must be BooleanArray");
        match &self.layout {
            BoolLayout::Fixed(dim) => {
                let d = *dim as usize;
                let start = i * d;
                Some(bool_arr.slice(start, d))
            }
            BoolLayout::Variable32(offsets) => {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                Some(bool_arr.slice(start, end - start))
            }
            BoolLayout::Variable64(offsets) => {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                Some(bool_arr.slice(start, end - start))
            }
        }
    }
}

fn validate_dims(a_len: usize, b_len: usize, row: usize) -> DaftResult<()> {
    if a_len != b_len {
        return Err(DaftError::RuntimeError(format!(
            "dimension mismatch at row {row}: {a_len} vs {b_len}"
        )));
    }
    Ok(())
}

/// Applies a pairwise float distance function over two list columns.
pub fn map_float_vectors<F>(a: &ArrayRef, b: &ArrayRef, f: F) -> DaftResult<ArrayRef>
where
    F: Fn(&[f64], &[f64]) -> f64,
{
    let col_a = FloatVectorColumn::try_from_array(a)?;
    let col_b = FloatVectorColumn::try_from_array(b)?;
    let mut builder = Float64Builder::with_capacity(col_a.len);
    for i in 0..col_a.len {
        match (col_a.get(i), col_b.get(i)) {
            (Some(va), Some(vb)) => {
                validate_dims(va.len(), vb.len(), i)?;
                builder.append_value(f(va, vb));
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Like map_float_vectors but the closure may return None (e.g. for zero-norm).
pub fn map_float_vectors_nullable<F>(a: &ArrayRef, b: &ArrayRef, f: F) -> DaftResult<ArrayRef>
where
    F: Fn(&[f64], &[f64]) -> Option<f64>,
{
    let col_a = FloatVectorColumn::try_from_array(a)?;
    let col_b = FloatVectorColumn::try_from_array(b)?;
    let mut builder = Float64Builder::with_capacity(col_a.len);
    for i in 0..col_a.len {
        match (col_a.get(i), col_b.get(i)) {
            (Some(va), Some(vb)) => {
                validate_dims(va.len(), vb.len(), i)?;
                match f(va, vb) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Applies a pairwise boolean distance function returning UInt32.
pub fn map_bool_vectors_u32<F>(a: &ArrayRef, b: &ArrayRef, f: F) -> DaftResult<ArrayRef>
where
    F: Fn(&BooleanArray, &BooleanArray) -> Option<u32>,
{
    let col_a = BoolVectorColumn::try_from_array(a)?;
    let col_b = BoolVectorColumn::try_from_array(b)?;
    let mut builder = UInt32Builder::with_capacity(col_a.len);
    for i in 0..col_a.len {
        match (col_a.get(i), col_b.get(i)) {
            (Some(va), Some(vb)) => {
                validate_dims(va.len(), vb.len(), i)?;
                match f(&va, &vb) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Applies a pairwise boolean distance function returning Float64.
pub fn map_bool_vectors_f64<F>(a: &ArrayRef, b: &ArrayRef, f: F) -> DaftResult<ArrayRef>
where
    F: Fn(&BooleanArray, &BooleanArray) -> Option<f64>,
{
    let col_a = BoolVectorColumn::try_from_array(a)?;
    let col_b = BoolVectorColumn::try_from_array(b)?;
    let mut builder = Float64Builder::with_capacity(col_a.len);
    for i in 0..col_a.len {
        match (col_a.get(i), col_b.get(i)) {
            (Some(va), Some(vb)) => {
                validate_dims(va.len(), vb.len(), i)?;
                match f(&va, &vb) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}
